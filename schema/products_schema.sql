CREATE TABLE IF NOT EXISTS product_dim (
    surrogate_key UUID PRIMARY KEY,
    product_id INT,
    product_name VARCHAR(255),
    category VARCHAR(255),
    price FLOAT,
    start_date DATE,
    end_date DATE
);