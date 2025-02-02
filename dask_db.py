from dask.distributed import LocalCluster, Client, get_worker, Lock  # Use Dask's Lock
import logging
import os
import random
import sqlite3
import json

# Configure logging
logging.basicConfig(filename='dask_master.log', level=logging.INFO)

# Database setup
DB_PATH = "pointclouds.db"

def setup_database():
    """Initialize the database schema."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pointclouds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            worker_id TEXT,
            worker_ip TEXT,
            worker_name TEXT,
            pointcloud TEXT
        )
    """)
    conn.commit()
    conn.close()

def generate_and_upload_pointcloud():
    """Worker generates a random point cloud and uploads it to the database."""
    try:
        # Get worker information
        worker = get_worker()
        worker_info = worker.worker_address.split(":")
        worker_id = worker_info[0]
        worker_ip = worker_info[1]
        worker_name = worker.name

        # Generate a random point cloud (list of 3D points)
        pointcloud = [[random.uniform(0, 100) for _ in range(3)] for _ in range(100)]

        logging.info(f"Worker {worker_name} generated a point cloud.")

        # Use the distributed lock to ensure safe database writes
        lock = Lock("db_lock")  # Use a distributed lock with a unique name
        with lock:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO pointclouds (worker_id, worker_ip, worker_name, pointcloud)
                VALUES (?, ?, ?, ?)
            """, (worker_id, worker_ip, worker_name, json.dumps(pointcloud)))
            conn.commit()
            conn.close()

        logging.info(f"Worker {worker_name} uploaded the point cloud to the database.")
        return f"Worker {worker_name} completed its task."

    except Exception as e:
        logging.error(f"Error in worker task: {e}")
        return f"Error in worker task: {e}"

def main():
    # Setup database
    setup_database()

    # Create Dask Server (Master Node) and 5 Worker Nodes
    cluster = LocalCluster(n_workers=5)
    client = Client(cluster)

    # Print Master Node and Worker Nodes addresses
    print("Master Node (Dask Server) address:", cluster.scheduler_address)
    workers_info = client.scheduler_info()['workers']
    print("\nWorker Nodes addresses:")
    for worker_addr in workers_info:
        print(f"- {worker_addr}")

    # Get the list of worker addresses
    worker_addresses = list(workers_info.keys())

    # Submit one task to each worker
    futures = []
    for worker_addr in worker_addresses:
        print(f'worker_addr:{worker_addr}')
        future = client.submit(generate_and_upload_pointcloud, workers=worker_addr)
        print(f'future:{future}')
        futures.append(future)

    # Gather results
    results = client.gather(futures)

    # Log results
    for result in results:
        logging.info(result)
        print(result)

    # Ensure resources are cleaned up
    client.close()
    cluster.close()

if __name__ == "__main__":
    main()