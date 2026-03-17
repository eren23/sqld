# VHS demo tape for sqld
# https://github.com/charmbracelet/vhs

Output demo.gif
Set Shell "bash"
Set FontSize 16
Set Width 1200
Set Height 750
Set Theme "Dracula"
Set TypingSpeed 40ms
Set Padding 20

# Start server invisibly
Hide
Type "cd /Users/eren/Documents/AI/attocodepy_swarmtester_10/sqld && ./target/release/sqld sqld_config.toml >/dev/null 2>&1 &"
Enter
Type "SQLD_PID=$!"
Enter
Sleep 2s
Type "clear"
Enter
Sleep 500ms
Show

# Connect with psql
Type "# sqld — a PostgreSQL-compatible database written from scratch in Rust"
Enter
Sleep 1s
Type "psql -h 127.0.0.1 -p 5433 -U sqld"
Enter
Sleep 1s

# Create tables
Type "CREATE TABLE users (id INTEGER, name VARCHAR(50), email VARCHAR(100), age INTEGER);"
Enter
Sleep 800ms

Type "CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, amount FLOAT);"
Enter
Sleep 800ms

# Insert data
Type "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 32);"
Enter
Sleep 500ms

Type "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 28);"
Enter
Sleep 500ms

Type "INSERT INTO users VALUES (3, 'Carol', 'carol@example.com', 35);"
Enter
Sleep 500ms

Type "INSERT INTO orders VALUES (1, 1, 'Widget', 29.99);"
Enter
Sleep 500ms

Type "INSERT INTO orders VALUES (2, 1, 'Gadget', 49.99);"
Enter
Sleep 500ms

Type "INSERT INTO orders VALUES (3, 2, 'Widget', 29.99);"
Enter
Sleep 500ms

Type "INSERT INTO orders VALUES (4, 3, 'Gizmo', 99.99);"
Enter
Sleep 800ms

# Basic SELECT
Type "SELECT * FROM users;"
Enter
Sleep 1.5s

# WHERE clause
Type "SELECT name, email FROM users WHERE age > 30;"
Enter
Sleep 1.5s

# JOIN
Type "SELECT u.name, o.product, o.amount FROM users u JOIN orders o ON u.id = o.user_id;"
Enter
Sleep 2s

# GROUP BY + HAVING
Type "SELECT u.name, COUNT(*) AS order_count, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name HAVING COUNT(*) > 1;"
Enter
Sleep 2s

# UPDATE
Type "UPDATE users SET age = 33 WHERE name = 'Alice';"
Enter
Sleep 800ms

Type "SELECT name, age FROM users WHERE name = 'Alice';"
Enter
Sleep 1.5s

# DELETE
Type "DELETE FROM orders WHERE product = 'Gizmo';"
Enter
Sleep 800ms

# SHOW TABLES
Type "SHOW TABLES;"
Enter
Sleep 1.5s

# EXPLAIN
Type "EXPLAIN SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 30;"
Enter
Sleep 2.5s

# Exit psql
Type "\\q"
Enter
Sleep 500ms

Type "# Built from scratch in Rust by an attocode AI swarm"
Enter
Sleep 2s

# Kill server invisibly
Hide
Type "kill $SQLD_PID 2>/dev/null; wait $SQLD_PID 2>/dev/null"
Enter
Sleep 500ms
Show
