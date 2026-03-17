use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::config::Config;
use crate::executor::CatalogProvider;
use crate::planner::Catalog;
use crate::protocol::connection::Connection;

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

pub struct Server {
    config: Config,
    catalog: Arc<Mutex<Catalog>>,
    catalog_provider: Arc<dyn CatalogProvider>,
    active_connections: Arc<AtomicUsize>,
    next_process_id: Arc<AtomicI32>,
    shutdown: Arc<AtomicBool>,
}

impl Server {
    pub fn new(
        config: Config,
        catalog: Arc<Mutex<Catalog>>,
        catalog_provider: Arc<dyn CatalogProvider>,
    ) -> Self {
        Self {
            config,
            catalog,
            catalog_provider,
            active_connections: Arc::new(AtomicUsize::new(0)),
            next_process_id: Arc::new(AtomicI32::new(1)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start listening for connections. Blocks until shutdown.
    pub fn run(&self) -> std::io::Result<()> {
        let addr = format!("{}:{}", self.config.server.host, self.config.server.port);
        let listener = TcpListener::bind(&addr)?;

        eprintln!(
            "sqld listening on {} (max_connections={})",
            addr, self.config.server.max_connections
        );

        // Accept connections
        for stream in listener.incoming() {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            let stream = match stream {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("accept error: {e}");
                    continue;
                }
            };

            // Connection limit check
            let active = self.active_connections.load(Ordering::Relaxed);
            if active >= self.config.server.max_connections {
                eprintln!("connection limit reached ({active}), rejecting");
                // We could send an error message, but for simplicity just drop
                drop(stream);
                continue;
            }

            let process_id = self.next_process_id.fetch_add(1, Ordering::Relaxed);
            let catalog = self.catalog.clone();
            let catalog_provider = self.catalog_provider.clone();
            let active_connections = self.active_connections.clone();

            active_connections.fetch_add(1, Ordering::Relaxed);

            thread::spawn(move || {
                match Connection::new(stream, catalog, catalog_provider, process_id) {
                    Ok(mut conn) => {
                        if let Err(e) = conn.run() {
                            eprintln!("connection {process_id} error: {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!("connection setup error: {e}");
                    }
                }
                active_connections.fetch_sub(1, Ordering::Relaxed);
            });
        }

        Ok(())
    }

    /// Signal the server to stop accepting connections.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Get the number of currently active connections.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }
}
