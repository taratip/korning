# Use this file to import the sales information into the
# the database.

require "pg"
require "pry"
require "csv"

def db_connection
  begin
    connection = PG.connect(dbname: "korning")
    yield(connection)
  ensure
    connection.close
  end
end

csv_records = CSV.foreach('sales.csv', headers: true)

db_connection do |conn|
  # iterate over CSV data
  csv_records.each do |record|
    employee = record["employee"]
    employee_detail = employee.split(' ')

    first_name = employee_detail[0]
    last_name = employee_detail[1]
    email = employee_detail[2].gsub(/[()]/,'')

    employee_results = conn.exec_params(
      'SELECT * FROM employees WHERE email = $1',
      [email]
    )

    if employee_results.to_a == []
      conn.exec_params(
        'INSERT INTO employees (first_name, last_name, email) VALUES ($1, $2, $3)',
        [first_name, last_name, email]
      )
    end

    customer = record["customer_and_account_no"]
    customer_detail = customer.split(' ')

    customer_name = customer_detail[0]
    customer_acct = customer_detail[1].gsub(/[()]/,'')

    customer_results = conn.exec_params(
      'SELECT * FROM customers WHERE account_number = $1',
      [customer_acct]
    )

    if customer_results.to_a == []
      conn.exec_params(
        'INSERT INTO customers (name, account_number) VALUES ($1, $2)',
        [customer_name, customer_acct]
      )
    end

    product_results = conn.exec_params(
      'SELECT * FROM products WHERE name = $1',
      [record["product_name"]]
    )

    if product_results.to_a == []
      conn.exec_params(
        'INSERT INTO products (name) VALUES ($1)',
        [record["product_name"]]
      )
    end

    frequency_results = conn.exec_params(
      'SELECT * FROM frequencies WHERE frequency = $1',
      [record["invoice_frequency"]]
    )

    if frequency_results.to_a == []
      conn.exec_params(
        'INSERT INTO frequencies (frequency) VALUES ($1)',
        [record["invoice_frequency"]]
      )
    end

    invoice_results = conn.exec_params(
      'SELECT * FROM invoices WHERE invoice_number = $1',
      [record["invoice_no"]]
    )

    if invoice_results.to_a == []
      conn.exec_params(
        'INSERT INTO invoices (invoice_number) VALUES ($1)',
        [record["invoice_no"]]
      )
    end

    sales_results = conn.exec_params(
      'SELECT * FROM sales s
      JOIN employees e ON s.employee_id = e.id
      JOIN customers c ON s.customer_id = c.id
      JOIN products p ON s.product_id = p.id
      JOIN invoices i ON s.invoice_id = i.id
      JOIN frequencies f ON s.frequency_id = f.id
      WHERE e.email = $1
      AND c.account_number = $2
      AND i.invoice_number = $3
      AND p.name = $4
      AND f.frequency = $5',
      [email, customer_acct, record["invoice_no"], record["product_name"], record["invoice_frequency"]]
    )

    if sales_results.to_a == []
      amount = record["sale_amount"].gsub(/[$]/,'')

      employee_id = conn.exec_params(
        'SELECT id FROM employees WHERE email = $1',
        [email]
      )[0]["id"]

      customer_id = conn.exec_params(
        'SELECT id FROM customers WHERE account_number = $1',
        [customer_acct]
      )[0]["id"]

      invoice_id = conn.exec_params(
        'SELECT id FROM invoices WHERE invoice_number = $1',
        [record["invoice_no"]]
      )[0]["id"]

      product_id = conn.exec_params(
        'SELECT id FROM products WHERE name = $1',
        [record["product_name"]]
      )[0]["id"]

      frequency_id = conn.exec_params(
        'SELECT * FROM frequencies WHERE frequency = $1',
        [record["invoice_frequency"]]
      )[0]["id"]

      conn.exec_params(
        'INSERT INTO sales (sale_date, sale_amount, unit_sold, employee_id, customer_id, product_id, invoice_id, frequency_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)',
        [record["sale_date"], amount, record["units_sold"], employee_id, customer_id, product_id, invoice_id, frequency_id]
      )
    end
  end
end
