DROP TABLE IF EXISTS transaction_detail;
CREATE TABLE IF NOT EXISTS transaction_detail (
    transaction_id VARCHAR(255),
    customer_id INTEGER,
    category VARCHAR(255), 
    brand VARCHAR(255),
    product_name VARCHAR(255), 
    quantity INTEGER,
    price INTEGER,
    payment_method VARCHAR(255), 
    timestamp TIMESTAMP 
);