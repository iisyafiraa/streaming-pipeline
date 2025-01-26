DROP TABLE IF EXISTS retail;
CREATE TABLE IF NOT EXISTS retail (
    category VARCHAR(255), 
    payment_method VARCHAR(255), 
    avg_price FLOAT,
    total_quantity INTEGER,
    total_amount INTEGER,
    running_total INTEGER,
    timestamp TIMESTAMP 
);