# Streaming for Data Pipeline

This repository demonstrates a complete data pipeline using **Apache Kafka**, **Apache Spark Structured Streaming**, and **PostgreSQL** to process and aggregate purchasing events in real-time.

## Prerequisites
Make sure the following dependencies are installed:
- Docker
- Jupyter (for local development and testing)
- PostgreSQL
- Kafka
- Apache Spark

## Run the Pipeline

### 1. **Build Docker Images**

To build the Docker images, run the following command:

```bash
make docker-build
```
This will create the necessary Docker images for PostgreSQL, Kafka, and Spark components.

### 2. **Start Jupyter Notebook**

For local development and testing, use Jupyter Notebook to interact with the Spark cluster and test code:

```bash
make jupyter
```

### 3. **Start PostgreSQL**

Start the PostgreSQL container to store the results:

```bash
make postgres
```

Make sure the PostgreSQL database is up and running to store aggregated results from the Spark job.

### 4. **Start Kafka**

Start the Kafka container to simulate a message broker:

```bash
make kafka
```

This will start the Kafka broker and create the necessary Kafka topics.

### 5. **Start Spark Streaming Job**
Run the Spark Streaming job that listens to the Kafka topic and aggregates the purchase data:

```bash
make spark
```

This will start the Spark job which listens to the Kafka topic, processes the incoming purchase events, and aggregates the data every 5 minutes.

## Event Producer

The Event Producer **generates purchasing events**. It simulates purchases with the following attributes:

| **Column Name**      | **Description**                                                       |
|----------------------|-----------------------------------------------------------------------|
| `transaction_id`     | Unique identifier for each transaction.                              |
| `customer_id`        | Identifier for the customer making the purchase.                     |
| `category`           | The category of the product purchased (e.g., Skincare, Makeup).      |
| `brand`              | The brand of the product.                                            |
| `product_name`       | The name of the product.                                             |
| `quantity`           | Number of items purchased.                                           |
| `price`              | Price per unit of the product.                                       |
| `payment_method`     | Payment method used (e.g., Credit Card, PayPal).                     |
| `timestamp (ts)`     | The timestamp of the event.                                          |

---

### Key Considerations:

The timestamp can be delayed by up to 1 hour. Ensure that the system accounts for late data.
Events are produced and sent to a Kafka topic for further processing.

## Streaming Job

The Streaming Job performs the following tasks:

### 1. Listens to the Kafka topic:

The Spark Structured Streaming job listens for incoming purchase events from the Kafka topic.

### 2. Aggregates purchase data:

Every 5 minutes, Spark will aggregate total purchases for each category and payment method. The aggregation includes:

| **Column Name**      | **Description**                                                       |
|----------------------|-----------------------------------------------------------------------|
| `category`           | The category of the product purchased.                                |
| `payment_method`     | The method of payment used for the transaction.                       |
| `avg_price`          | The average price for the category and payment method.                |
| `total_quantity`     | The total quantity of products purchased in the aggregated window.    |
| `total_amount`       | The total amount spent (quantity * price).                            |

### 3. Handles Late Data:

The system is configured to **allow late data (up to 1 hour)** using Spark's watermarking feature. This ensures that even if data arrives later than expected, it will still be processed correctly.

### 4. Writes aggregated data to the console:

After aggregation, the job outputs the following columns to the console:

| **Column Name**      | **Description**                                                       |
|----------------------|-----------------------------------------------------------------------|
| `running_total`      | The running total of the total_amount up to the current timestamp.    |
| `timestamp`          | The timestamp of the aggregation (when the aggregation was computed). |

So, the result in retail table is like this:

| **Column Name**      | **Description**                                                       |
|----------------------|-----------------------------------------------------------------------|
| `category`           | The category of the product purchased.                                |
| `payment_method`     | The method of payment used for the transaction.                       |
| `avg_price`          | The average price for the category and payment method.                |
| `total_quantity`     | The total quantity of products purchased in the aggregated window.    |
| `total_amount`       | The total amount spent (quantity * price).                            |
| `running_total`      | The running total of the total_amount up to the current timestamp.    |
| `timestamp`          | The timestamp of the aggregation (when the aggregation was computed). |

The aggregation is **performed every 5 minutes** (as defined by the trigger interval). This ensures that the system processes and outputs the total purchase data at regular intervals.
Late data handling is crucial in this pipeline. Spark allows for late data to be processed, even if the events are delayed by up to 1 hour. This is managed by using watermarks on the timestamp column.

## Troubleshooting

If you encounter any issues, ensure the following:
1. Kafka is running and the topic is correctly configured.
2. PostgreSQL is running and accessible.
3. The Spark job has the necessary permissions to write to PostgreSQL.
4. Check for any firewall or connection issues between the components (Kafka, Spark, PostgreSQL).

Let me know if you need further adjustments!