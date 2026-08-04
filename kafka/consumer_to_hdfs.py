from kafka import KafkaConsumer
import subprocess

consumer = KafkaConsumer(
    "hm-raw",
    bootstrap_servers="localhost:9092",
    group_id="hdfs-loader",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    max_poll_records=5000
)

buffers = {}
sizes = {}

BATCH_SIZE = 64 * 1024 * 1024

def write_batch(file_name, data):
    subprocess.run(
        [
            "docker", "exec", "-i", "namenode",
            "hdfs", "dfs", "-appendToFile",
            "-", f"/data/hm/raw/{file_name}"
        ],
        input=data,
        check=True
    )

for message in consumer:
    file_name, data = message.value.split(b"\t", 1)
    file_name = file_name.decode()

    if file_name not in buffers:
        buffers[file_name] = []
        sizes[file_name] = 0

    buffers[file_name].append(data + b"\n")
    sizes[file_name] += len(data) + 1

    if sizes[file_name] >= BATCH_SIZE:
        write_batch(file_name, b"".join(buffers[file_name]))
        buffers[file_name] = []
        sizes[file_name] = 0

        consumer.commit()
        print(f"Loaded: {file_name}")

for file_name in buffers:
    if buffers[file_name]:
        write_batch(file_name, b"".join(buffers[file_name]))

consumer.commit()
consumer.close()
