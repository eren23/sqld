use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use crate::executor::CatalogProvider;
use crate::planner::Catalog;
use crate::protocol::extended_query::{self, Portal, PreparedStatement};
use crate::protocol::messages::{
    read_frontend_message, read_startup_message, BackendMessage, ErrorFields,
    FrontendMessage, Severity, TransactionState,
};
use crate::protocol::simple_query;

// ---------------------------------------------------------------------------
// Session — per-connection state
// ---------------------------------------------------------------------------

pub struct Session {
    pub user: String,
    pub database: String,
    pub txn_state: TransactionState,
    pub catalog: Arc<Mutex<Catalog>>,
    pub catalog_provider: Arc<dyn CatalogProvider>,
    pub prepared_statements: HashMap<String, PreparedStatement>,
    pub portals: HashMap<String, Portal>,
    pub process_id: i32,
    pub secret_key: i32,
    pub params: HashMap<String, String>,
}

impl Session {
    pub fn new(
        catalog: Arc<Mutex<Catalog>>,
        catalog_provider: Arc<dyn CatalogProvider>,
        process_id: i32,
    ) -> Self {
        Self {
            user: String::new(),
            database: String::new(),
            txn_state: TransactionState::Idle,
            catalog,
            catalog_provider,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            process_id,
            secret_key: rand_i32(),
            params: default_params(),
        }
    }
}

fn default_params() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("server_version".to_string(), "0.1.0".to_string());
    m.insert("server_encoding".to_string(), "UTF8".to_string());
    m.insert("client_encoding".to_string(), "UTF8".to_string());
    m.insert("DateStyle".to_string(), "ISO, MDY".to_string());
    m.insert("TimeZone".to_string(), "UTC".to_string());
    m.insert("integer_datetimes".to_string(), "on".to_string());
    m.insert("standard_conforming_strings".to_string(), "on".to_string());
    m
}

fn rand_i32() -> i32 {
    use std::time::SystemTime;
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_nanos() & 0x7FFF_FFFF) as i32
}

// ---------------------------------------------------------------------------
// Connection — wraps a TCP stream + session
// ---------------------------------------------------------------------------

pub struct Connection {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    pub session: Session,
}

impl Connection {
    pub fn new(
        stream: TcpStream,
        catalog: Arc<Mutex<Catalog>>,
        catalog_provider: Arc<dyn CatalogProvider>,
        process_id: i32,
    ) -> io::Result<Self> {
        let reader = BufReader::new(stream.try_clone()?);
        let writer = BufWriter::new(stream);
        let session = Session::new(catalog, catalog_provider, process_id);
        Ok(Self {
            reader,
            writer,
            session,
        })
    }

    /// Run the connection lifecycle: startup → ready → process messages → terminate.
    pub fn run(&mut self) -> io::Result<()> {
        self.handle_startup()?;
        self.message_loop()?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Startup phase
    // -----------------------------------------------------------------------

    fn handle_startup(&mut self) -> io::Result<()> {
        let msg = read_startup_message(&mut self.reader)?;

        match msg {
            FrontendMessage::SslRequest => {
                // Deny SSL — send 'N'
                self.writer.write_all(&[b'N'])?;
                self.writer.flush()?;
                // Client will retry with a regular startup
                return self.handle_startup();
            }
            FrontendMessage::Startup { version: _, params } => {
                self.session.user = params.get("user").cloned().unwrap_or_default();
                self.session.database = params
                    .get("database")
                    .cloned()
                    .unwrap_or_else(|| self.session.user.clone());

                // AuthenticationOk (no password required)
                self.send(&BackendMessage::AuthenticationOk)?;

                // Send parameter statuses
                let params: Vec<(String, String)> = self
                    .session
                    .params
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                for (name, value) in params {
                    self.send(&BackendMessage::ParameterStatus { name, value })?;
                }

                // BackendKeyData
                self.send(&BackendMessage::BackendKeyData {
                    process_id: self.session.process_id,
                    secret_key: self.session.secret_key,
                })?;

                // ReadyForQuery
                self.send(&BackendMessage::ReadyForQuery {
                    state: TransactionState::Idle,
                })?;

                self.writer.flush()?;
            }
            FrontendMessage::CancelRequest { .. } => {
                // Cancel requests don't get a response
                return Ok(());
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected message during startup",
                ));
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Main message loop
    // -----------------------------------------------------------------------

    fn message_loop(&mut self) -> io::Result<()> {
        loop {
            let msg = match read_frontend_message(&mut self.reader) {
                Ok(m) => m,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    return Ok(()); // client disconnected
                }
                Err(e) => return Err(e),
            };

            match msg {
                FrontendMessage::Terminate => return Ok(()),

                FrontendMessage::Query { sql } => {
                    let messages =
                        simple_query::handle_simple_query(&sql, &mut self.session);
                    for m in &messages {
                        self.send(m)?;
                    }
                    self.writer.flush()?;
                }

                FrontendMessage::Parse {
                    name,
                    query,
                    param_types,
                } => {
                    let resp = extended_query::handle_parse(
                        &name,
                        &query,
                        &param_types,
                        &mut self.session,
                    );
                    self.send(&resp)?;
                }

                FrontendMessage::Bind {
                    portal,
                    statement,
                    param_formats,
                    param_values,
                    result_formats,
                } => {
                    let resp = extended_query::handle_bind(
                        &portal,
                        &statement,
                        &param_formats,
                        &param_values,
                        &result_formats,
                        &mut self.session,
                    );
                    self.send(&resp)?;
                }

                FrontendMessage::Describe { target, name } => {
                    let messages =
                        extended_query::handle_describe(target, &name, &self.session);
                    for m in &messages {
                        self.send(m)?;
                    }
                }

                FrontendMessage::Execute { portal, max_rows } => {
                    let messages = extended_query::handle_execute(
                        &portal,
                        max_rows,
                        &mut self.session,
                    );
                    for m in &messages {
                        self.send(m)?;
                    }
                }

                FrontendMessage::Close { target, name } => {
                    let resp =
                        extended_query::handle_close(target, &name, &mut self.session);
                    self.send(&resp)?;
                }

                FrontendMessage::Sync => {
                    let resp = extended_query::handle_sync(&self.session);
                    self.send(&resp)?;
                    self.writer.flush()?;
                }

                FrontendMessage::Flush => {
                    self.writer.flush()?;
                }

                FrontendMessage::CopyData { data: _data } => {
                    // COPY data handled in copy mode state
                    // For now, buffer it
                }

                FrontendMessage::CopyDone => {
                    // End of COPY IN stream
                }

                FrontendMessage::CopyFail { message } => {
                    self.send(&BackendMessage::ErrorResponse(
                        ErrorFields::new(Severity::Error, "57014", message),
                    ))?;
                    self.send(&BackendMessage::ReadyForQuery {
                        state: self.session.txn_state,
                    })?;
                    self.writer.flush()?;
                }

                _ => {
                    self.send(&BackendMessage::ErrorResponse(
                        ErrorFields::feature_not_supported("unsupported message type"),
                    ))?;
                    self.writer.flush()?;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Send helper
    // -----------------------------------------------------------------------

    fn send(&mut self, msg: &BackendMessage) -> io::Result<()> {
        msg.encode(&mut self.writer)
    }
}
