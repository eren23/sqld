-- ============================================================
-- joins.sql
-- Tests: INNER JOIN, LEFT JOIN, RIGHT JOIN, CROSS JOIN,
--        multi-table joins, self join, aliases, and
--        complex ON conditions.
-- ============================================================

-- Setup: employees table
CREATE TABLE employees (
    id INTEGER,
    name TEXT,
    dept_id INTEGER
);

-- Setup: departments table
CREATE TABLE departments (
    id INTEGER,
    dept_name TEXT
);

-- Setup: projects table
CREATE TABLE projects (
    id INTEGER,
    project_name TEXT,
    lead_id INTEGER
);

INSERT INTO departments VALUES (1, 'Engineering');
INSERT INTO departments VALUES (2, 'Marketing');
INSERT INTO departments VALUES (3, 'Sales');

INSERT INTO employees VALUES (1, 'Alice', 1);
INSERT INTO employees VALUES (2, 'Bob', 1);
INSERT INTO employees VALUES (3, 'Charlie', 2);
INSERT INTO employees VALUES (4, 'Diana', NULL);

INSERT INTO projects VALUES (100, 'Alpha', 1);
INSERT INTO projects VALUES (101, 'Beta', 2);
INSERT INTO projects VALUES (102, 'Gamma', 5);

-- Test: INNER JOIN (only matching rows)
SELECT e.name, d.dept_name
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id;

-- Test: LEFT JOIN (all employees, even without a department)
SELECT e.name, d.dept_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id;

-- Test: RIGHT JOIN (all departments, even without employees)
SELECT e.name, d.dept_name
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id;

-- Test: CROSS JOIN (cartesian product)
SELECT e.name, d.dept_name
FROM employees e
CROSS JOIN departments d;

-- Test: Multi-table join (employees -> departments -> projects via lead)
SELECT e.name, d.dept_name, p.project_name
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
LEFT JOIN projects p ON p.lead_id = e.id;

-- Test: Self join (employees in the same department)
SELECT e1.name AS emp1, e2.name AS emp2, e1.dept_id
FROM employees e1
INNER JOIN employees e2 ON e1.dept_id = e2.dept_id AND e1.id < e2.id;

-- Test: Join with WHERE clause filtering
SELECT e.name, d.dept_name
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
WHERE d.dept_name = 'Engineering';

-- Test: Join with complex ON condition
SELECT e.name, p.project_name
FROM employees e
LEFT JOIN projects p ON p.lead_id = e.id AND p.project_name <> 'Gamma';

-- Test: Join using table aliases only
SELECT a.name, b.dept_name
FROM employees a, departments b
WHERE a.dept_id = b.id;

-- Cleanup
DROP TABLE projects;
DROP TABLE employees;
DROP TABLE departments;
