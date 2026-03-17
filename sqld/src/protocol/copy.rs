use crate::protocol::connection::Session;
use crate::protocol::messages::{BackendMessage, ErrorFields};
use crate::sql::ast::{Copy, CopyDirection};
use crate::types::{Datum, DataType};

// ---------------------------------------------------------------------------
// COPY configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CopyOptions {
    pub delimiter: u8,
    pub has_header: bool,
    pub null_string: String,
    pub format: CopyFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Csv,
    Text,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            has_header: false,
            null_string: String::new(),
            format: CopyFormat::Csv,
        }
    }
}

impl CopyOptions {
    pub fn csv() -> Self {
        Self {
            delimiter: b',',
            has_header: true,
            null_string: String::new(),
            format: CopyFormat::Csv,
        }
    }

    pub fn with_delimiter(mut self, d: u8) -> Self {
        self.delimiter = d;
        self
    }

    pub fn with_header(mut self, h: bool) -> Self {
        self.has_header = h;
        self
    }
}

// ---------------------------------------------------------------------------
// COPY FROM (import)
// ---------------------------------------------------------------------------

/// Handle a COPY statement in simple query mode.
/// For COPY FROM, reads from a file path (simplified; in PG this uses the
/// COPY protocol messages for streaming data).
/// For COPY TO, generates CSV output as CopyData messages.
pub fn handle_copy_statement(
    copy: &Copy,
    session: &mut Session,
) -> Result<Vec<BackendMessage>, ErrorFields> {
    let catalog = session.catalog.lock().unwrap();
    let schema = catalog.get_schema(&copy.table).ok_or_else(|| {
        ErrorFields::undefined_table(format!("table \"{}\" does not exist", copy.table))
    })?;
    let schema = schema.clone();
    drop(catalog);

    // Determine which columns to use
    let columns = if let Some(ref cols) = copy.columns {
        cols.clone()
    } else {
        schema.columns().iter().map(|c| c.name.clone()).collect()
    };

    let col_types: Vec<DataType> = columns
        .iter()
        .map(|name| {
            schema
                .columns()
                .iter()
                .find(|c| c.name == *name)
                .map(|c| c.data_type)
                .unwrap_or(DataType::Text)
        })
        .collect();

    match &copy.direction {
        CopyDirection::From(path) => {
            handle_copy_from(path, &copy.table, &columns, &col_types, session)
        }
        CopyDirection::To(path) => {
            handle_copy_to(path, &copy.table, &columns, &col_types, session)
        }
    }
}

fn handle_copy_from(
    path: &str,
    table: &str,
    _columns: &[String],
    col_types: &[DataType],
    session: &mut Session,
) -> Result<Vec<BackendMessage>, ErrorFields> {
    let data = std::fs::read_to_string(path).map_err(|e| {
        ErrorFields::internal(format!("could not read file '{}': {}", path, e))
    })?;

    let options = CopyOptions::csv();
    let mut row_count = 0u64;
    let delimiter = options.delimiter as char;

    for (line_no, line) in data.lines().enumerate() {
        // Skip header line
        if line_no == 0 && options.has_header {
            continue;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed == "\\." {
            continue;
        }

        let fields = parse_csv_line(trimmed, delimiter);
        if fields.len() != col_types.len() {
            return Err(ErrorFields::data_exception(format!(
                "wrong number of fields on line {}: expected {}, got {}",
                line_no + 1,
                col_types.len(),
                fields.len()
            )));
        }

        let values: Vec<Datum> = fields
            .iter()
            .zip(col_types.iter())
            .map(|(field, dt)| {
                if field.is_empty() || field == &options.null_string {
                    Ok(Datum::Null)
                } else {
                    crate::protocol::messages::text_to_datum(field.as_bytes(), dt)
                        .map_err(|e| ErrorFields::data_exception(format!("line {}: {e}", line_no + 1)))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Insert the tuple
        session
            .catalog_provider
            .insert_tuple(table, values)
            .map_err(|e| ErrorFields::internal(format!("insert error: {e}")))?;

        row_count += 1;
    }

    Ok(vec![BackendMessage::CommandComplete {
        tag: format!("COPY {row_count}"),
    }])
}

fn handle_copy_to(
    path: &str,
    table: &str,
    columns: &[String],
    col_types: &[DataType],
    session: &mut Session,
) -> Result<Vec<BackendMessage>, ErrorFields> {
    let tuples = session
        .catalog_provider
        .scan_table(table)
        .map_err(|e| ErrorFields::internal(format!("scan error: {e}")))?;

    let options = CopyOptions::csv();
    let delimiter = options.delimiter as char;
    let mut output = String::new();

    // Header
    if options.has_header {
        output.push_str(&columns.join(&delimiter.to_string()));
        output.push('\n');
    }

    let mut row_count = 0u64;
    for tuple in &tuples {
        let data = tuple.values();
        let fields: Vec<String> = data
            .iter()
            .map(|d| {
                crate::protocol::messages::datum_to_text(d)
                    .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
                    .unwrap_or_default()
            })
            .collect();
        output.push_str(&fields.join(&delimiter.to_string()));
        output.push('\n');
        row_count += 1;
    }

    // Write to file or return as messages
    if path == "STDOUT" || path == "stdout" {
        // Return as CopyOutResponse + CopyData + CopyDone
        let column_formats: Vec<i16> = col_types.iter().map(|_| 0i16).collect();
        let mut messages = vec![BackendMessage::CopyOutResponse {
            format: 0,
            column_formats,
        }];

        for line in output.lines() {
            let mut data = line.as_bytes().to_vec();
            data.push(b'\n');
            messages.push(BackendMessage::CopyData { data });
        }

        messages.push(BackendMessage::CopyDone);
        messages.push(BackendMessage::CommandComplete {
            tag: format!("COPY {row_count}"),
        });
        Ok(messages)
    } else {
        std::fs::write(path, &output).map_err(|e| {
            ErrorFields::internal(format!("could not write file '{}': {}", path, e))
        })?;

        Ok(vec![BackendMessage::CommandComplete {
            tag: format!("COPY {row_count}"),
        }])
    }
}

// ---------------------------------------------------------------------------
// COPY protocol (streaming mode via CopyData messages)
// ---------------------------------------------------------------------------

/// Initiate COPY IN (streaming from client).
/// Returns the CopyInResponse to send to the client.
pub fn begin_copy_in(
    _table: &str,
    _columns: &[String],
    col_types: &[DataType],
    _options: &CopyOptions,
) -> BackendMessage {
    let column_formats: Vec<i16> = col_types.iter().map(|_| 0i16).collect();
    BackendMessage::CopyInResponse {
        format: 0,
        column_formats,
    }
}

/// Process incoming CopyData during COPY IN.
/// Parses CSV lines and returns parsed rows.
pub fn process_copy_data(
    data: &[u8],
    col_types: &[DataType],
    options: &CopyOptions,
) -> Result<Vec<Vec<Datum>>, ErrorFields> {
    let text = std::str::from_utf8(data)
        .map_err(|e| ErrorFields::data_exception(format!("invalid UTF-8 in COPY data: {e}")))?;

    let delimiter = options.delimiter as char;
    let mut rows = Vec::new();

    for (line_no, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed == "\\." {
            continue;
        }

        let fields = parse_csv_line(trimmed, delimiter);
        if fields.len() != col_types.len() {
            return Err(ErrorFields::data_exception(format!(
                "wrong number of fields on line {}: expected {}, got {}",
                line_no + 1,
                col_types.len(),
                fields.len()
            )));
        }

        let values: Vec<Datum> = fields
            .iter()
            .zip(col_types.iter())
            .map(|(field, dt)| {
                if field.is_empty() || *field == options.null_string {
                    Ok(Datum::Null)
                } else {
                    crate::protocol::messages::text_to_datum(field.as_bytes(), dt)
                        .map_err(|e| ErrorFields::data_exception(e))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        rows.push(values);
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// CSV parsing
// ---------------------------------------------------------------------------

/// Parse a CSV line respecting quoted fields.
fn parse_csv_line(line: &str, delimiter: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    // Escaped quote
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                current.push(ch);
            }
        } else if ch == '"' {
            in_quotes = true;
        } else if ch == delimiter {
            fields.push(current.clone());
            current.clear();
        } else {
            current.push(ch);
        }
    }
    fields.push(current);
    fields
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_csv() {
        let fields = parse_csv_line("a,b,c", ',');
        assert_eq!(fields, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_csv_quoted() {
        let fields = parse_csv_line(r#""hello, world",42,"test""#, ',');
        assert_eq!(fields, vec!["hello, world", "42", "test"]);
    }

    #[test]
    fn parse_csv_escaped_quotes() {
        let fields = parse_csv_line(r#""he said ""hi""",done"#, ',');
        assert_eq!(fields, vec![r#"he said "hi""#, "done"]);
    }

    #[test]
    fn parse_csv_tab_delimiter() {
        let fields = parse_csv_line("a\tb\tc", '\t');
        assert_eq!(fields, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_csv_empty_fields() {
        let fields = parse_csv_line(",hello,,world,", ',');
        assert_eq!(fields, vec!["", "hello", "", "world", ""]);
    }
}
