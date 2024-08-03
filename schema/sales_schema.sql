CREATE TABLE IF NOT EXISTS sales_fact (
    sale_id INT PRIMARY KEY,
    product_sk UUID,
    customer_sk UUID,
    quantity INT,
    sale_date DATE,
    updated_at TIMESTAMP,
    FOREIGN KEY (product_sk) REFERENCES product_dim(surrogate_key),
    FOREIGN KEY (customer_sk) REFERENCES customer_dim(surrogate_key)
);