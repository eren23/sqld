use sqld::protocol::messages::BackendMessage;

/// Assert that `messages` contains at least one `ErrorResponse` whose
/// SQLSTATE code matches `expected_code`.
///
/// # Panics
///
/// Panics if no matching `ErrorResponse` is found.
pub fn assert_contains_error(messages: &[BackendMessage], expected_code: &str) {
    let found = messages.iter().any(|m| {
        matches!(m, BackendMessage::ErrorResponse(e) if e.code == expected_code)
    });
    assert!(
        found,
        "expected ErrorResponse with SQLSTATE '{expected_code}', but none found.\n\
         Messages: {messages:#?}"
    );
}

/// Alias for [`assert_contains_error`] -- find an `ErrorResponse` with the
/// given SQLSTATE code.
pub fn assert_error_code(messages: &[BackendMessage], code: &str) {
    assert_contains_error(messages, code);
}

/// Assert that at least one `ErrorResponse` exists in `messages`.
///
/// # Panics
///
/// Panics if no `ErrorResponse` is found.
pub fn assert_has_error(messages: &[BackendMessage]) {
    let found = messages
        .iter()
        .any(|m| matches!(m, BackendMessage::ErrorResponse(_)));
    assert!(
        found,
        "expected at least one ErrorResponse, but none found.\nMessages: {messages:#?}"
    );
}

/// Assert that no `ErrorResponse` exists in `messages`.
///
/// # Panics
///
/// Panics if any `ErrorResponse` is found.
pub fn assert_no_error(messages: &[BackendMessage]) {
    for msg in messages {
        if let BackendMessage::ErrorResponse(err) = msg {
            panic!(
                "expected no ErrorResponse, but found: [{}] {}\nMessages: {messages:#?}",
                err.code, err.message
            );
        }
    }
}

/// Assert that `messages` contains a `CommandComplete` whose tag starts with
/// `tag_prefix`.
///
/// # Panics
///
/// Panics if no matching `CommandComplete` is found.
pub fn assert_command_complete(messages: &[BackendMessage], tag_prefix: &str) {
    let found = messages.iter().any(|m| {
        matches!(m, BackendMessage::CommandComplete { tag } if tag.starts_with(tag_prefix))
    });
    assert!(
        found,
        "expected CommandComplete with tag starting with '{tag_prefix}', but none found.\n\
         Messages: {messages:#?}"
    );
}

/// Assert that exactly `expected` `DataRow` messages appear in `messages`.
///
/// # Panics
///
/// Panics if the count does not match.
pub fn assert_row_count(messages: &[BackendMessage], expected: usize) {
    let actual = messages
        .iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count();
    assert_eq!(
        actual, expected,
        "expected {expected} DataRow messages, found {actual}"
    );
}

/// Assert that the first `RowDescription` in `messages` contains columns
/// whose names match `expected_names` (in order).
///
/// # Panics
///
/// Panics if no `RowDescription` is found or the names differ.
pub fn assert_column_names(messages: &[BackendMessage], expected_names: &[&str]) {
    let row_desc = messages.iter().find_map(|m| match m {
        BackendMessage::RowDescription { fields } => Some(fields),
        _ => None,
    });

    let fields = row_desc.expect(
        "expected a RowDescription message, but none found"
    );

    let actual_names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        actual_names, expected_names,
        "column names mismatch"
    );
}

/// Extract all `DataRow` messages from `messages`, converting each column
/// value to `Option<String>` (`None` for SQL NULL).
pub fn extract_rows(messages: &[BackendMessage]) -> Vec<Vec<Option<String>>> {
    messages
        .iter()
        .filter_map(|m| match m {
            BackendMessage::DataRow { values } => {
                let row: Vec<Option<String>> = values
                    .iter()
                    .map(|v| {
                        v.as_ref()
                            .map(|bytes| String::from_utf8_lossy(bytes).to_string())
                    })
                    .collect();
                Some(row)
            }
            _ => None,
        })
        .collect()
}

/// Assert that the data rows extracted from `messages` match `expected`
/// exactly (order-sensitive).
///
/// Each inner slice represents one row where each element is the expected
/// text representation of the column value, or `None` for SQL NULL.
///
/// # Panics
///
/// Panics if the extracted rows differ from `expected`.
pub fn assert_rows_eq(messages: &[BackendMessage], expected: &[Vec<Option<&str>>]) {
    let actual = extract_rows(messages);

    let expected_owned: Vec<Vec<Option<String>>> = expected
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| v.map(|s| s.to_string()))
                .collect()
        })
        .collect();

    assert_eq!(
        actual, expected_owned,
        "row data mismatch\n  actual:   {actual:#?}\n  expected: {expected_owned:#?}"
    );
}

/// Assert that the `row_idx`-th `DataRow` message (zero-based) contains the
/// given text values (in order). NULL columns should be represented by the
/// string `"NULL"` in `expected_values`.
///
/// # Panics
///
/// Panics if the row index is out of bounds or the values differ.
pub fn assert_row_values(
    messages: &[BackendMessage],
    row_idx: usize,
    expected_values: &[&str],
) {
    let rows = extract_rows(messages);
    assert!(
        row_idx < rows.len(),
        "row index {row_idx} out of range (only {} DataRow messages)",
        rows.len()
    );
    let actual: Vec<String> = rows[row_idx]
        .iter()
        .map(|v| match v {
            Some(s) => s.clone(),
            None => "NULL".to_string(),
        })
        .collect();
    let expected: Vec<String> = expected_values.iter().map(|s| s.to_string()).collect();
    assert_eq!(
        actual, expected,
        "row {row_idx} values mismatch\n  actual:   {actual:?}\n  expected: {expected:?}"
    );
}

/// Extract the tag from the first `CommandComplete` message, or `None` if no
/// such message exists.
pub fn extract_command_tag(messages: &[BackendMessage]) -> Option<String> {
    messages.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    })
}
