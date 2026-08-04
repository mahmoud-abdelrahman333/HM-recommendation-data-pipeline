from kafka import KafkaProducer
import os

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    batch_size=1024 * 1024,
    linger_ms=20,
    compression_type="gzip",
    max_request_size=10 * 1024 * 1024
)

files = [
    "/mnt/hgfs/HM/articles.csv",
    "/mnt/hgfs/HM/customers.csv",
    "/mnt/hgfs/HM/transactions_train.csv"
]

for file_path in files:
    file_name = os.path.basename(file_path)
    print(f"Starting: {file_name}")

    with open(file_path, "rb") as f:
        for line in f:
            message = file_name.encode() + b"\t" + line.rstrip(b"\n")
            producer.send("hm-raw", message)

    producer.flush()
    print(f"Finished: {file_name}")

producer.close()
