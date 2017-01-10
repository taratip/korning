-- DEFINE YOUR DATABASE SCHEMA HERE
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS invoices;
DROP TABLE IF EXISTS frequencies;
DROP TABLE IF EXISTS products;

CREATE TABLE employees (
  id SERIAL PRIMARY KEY,
  first_name VARCHAR(255),
  last_name VARCHAR(255),
  email VARCHAR(255)
);

CREATE TABLE customers (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  account_number VARCHAR(255)
);

CREATE TABLE frequencies (
  id SERIAL PRIMARY KEY,
  frequency VARCHAR(255)
);

CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255)
);

CREATE TABLE invoices (
  id SERIAL PRIMARY KEY,
  invoice_number INT
);

CREATE TABLE sales (
  id SERIAL PRIMARY KEY,
  sale_date DATE,
  sale_amount NUMERIC,
  unit_sold INT,
  employee_id INT REFERENCES employees(id),
  customer_id INT REFERENCES customers(id),
  product_id INT REFERENCES products(id),
  invoice_id INT REFERENCES invoices(id),
  frequency_id INT REFERENCES frequencies(id)
);
