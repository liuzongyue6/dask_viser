from dask.distributed import LocalCluster, Client
import logging
import os

# Configure logging
logging.basicConfig(filename='dask_toy_master.log', level=logging.INFO)

def write_file(worker_address):
    """Worker creates a file and writes its own IP address"""
    try:
        # Extract the IP address from the worker address
        worker_port = worker_address.split("://")[1].split(":")[-1]
        current_dir = os.getcwd()
        
        # Create a file and write the IP address
        file_path = os.path.join(current_dir, f"worker_{worker_port}.txt")
        with open(file_path, "w") as f:
            f.write(f"This file was created by worker with IP: {worker_address}")
        
        return file_path
    
    except Exception as e:
        logging.error(f"Error writing file for worker {worker_address}: {e}")
        return None

def read_file(file_path):
    """Worker reads a file and returns its content"""
    try:
        with open(file_path, "r") as f:
            return f.read()
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return None

def main():
    # Create a local Dask cluster with 2 workers
    cluster = LocalCluster(n_workers=2)
    client = Client(cluster)
    
    try:
        print("Master Node (Dask Server) address:", cluster.scheduler_address)
        
        # Get all worker addresses
        workers_info = client.scheduler_info()['workers']
        print("\nWorker Nodes addresses:")
        for worker_addr in workers_info:
            print(f"- {worker_addr}")
        
        # Submit tasks to each worker to create a file and write its IP address
        futures = []  # Collect all futures in a single list
        for worker_addr in workers_info:
            future = client.submit(write_file, worker_addr, workers=[worker_addr])
            futures.append(future)
        
        # Gather file paths from all workers
        file_paths = client.gather(futures)
        
        # Submit tasks to each worker to read the file content
        read_futures = []
        for file_path in file_paths:
            future = client.submit(read_file, file_path)
            read_futures.append(future)
        
        # Gather results from all workers
        results = client.gather(read_futures)

        # Log the results and check if any content is missing
        missing_content = False
        for i, result in enumerate(results):
            if result:
                logging.info(f"Worker {i} content: {result}")
            else:
                logging.warning(f"Worker {i} content is missing!")
                missing_content = True

        if missing_content:
            print("Warning: Some content is missing. Check dask_master.log for details.")
        else:
            print("All content received successfully.")
            
    finally:
        # Ensure resources are cleaned up
        client.close()
        cluster.close()

if __name__ == "__main__":
    main()