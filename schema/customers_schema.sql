CREATE TABLE IF NOT EXISTS customer_dim (
    surrogate_key UUID PRIMARY KEY,
    customer_id INT,
    customer_name VARCHAR(255),
    email VARCHAR(255),
    start_date DATE,
    end_date DATE
);