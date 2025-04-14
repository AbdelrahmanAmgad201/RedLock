import redis
import time
import uuid
import multiprocessing

client_processes_waiting = [0, 5, 6, 10, 12]

class Redlock:
    def __init__(self, redis_cluster_nodes):
        # Create connection 
        self.redis_connections = []
        for hostname, port_number in redis_cluster_nodes:
            connection = redis.Redis(
                host=hostname, 
                port=port_number, 
                decode_responses=True,
                socket_connect_timeout=2.0
            )
            self.redis_connections.append(connection)
            
        # target
        self.quorum_size = (len(self.redis_connections) // 2) + 1
        
        # ttl time
        self.default_expiry = 5000
        
    def acquire_lock(self, resource_key, expiry_time_ms):
        unique_token = f"lock:{str(uuid.uuid4())}"
        deadline = time.time() + (expiry_time_ms / 1000.0) * 0.5
        successful_locks = 0
        
        # Try to acquire lock 
        for redis_instance in self.redis_connections:
            try:
                if redis_instance.set(
                    resource_key,
                    unique_token,
                    nx=True,
                    px=expiry_time_ms
                ):
                    successful_locks += 1
            except redis.exceptions.ConnectionError:
                print("Connection failed during lock acquisition")
        
        # Check if we got enough locks 
        if time.time() <= deadline and successful_locks >= self.quorum_size:
            return True, unique_token
        
        # If not enough locks were acquired, clean up
        self._clean_lock(resource_key, unique_token)
        return False, None
    
    def release_lock(self, resource_key, lock_token):
        self._clean_lock(resource_key, lock_token)
    
    def _clean_lock(self, resource_key, lock_token):
        for redis_instance in self.redis_connections:
            try:
                current_value = redis_instance.get(resource_key)
                if current_value and current_value == lock_token:
                    # Remove our lock
                    redis_instance.delete(resource_key)
            except redis.exceptions.ConnectionError:
                print("Connection failed during lock cleanup")

def client_process(redis_nodes, resource, ttl, client_id):
    """
    Function to simulate a single client process trying to acquire and release a lock.
    """
    time.sleep(client_processes_waiting[client_id])

    redlock = Redlock(redis_nodes)
    print(f"\nClient-{client_id}: Attempting to acquire lock...")
    lock_acquired, lock_id = redlock.acquire_lock(resource, ttl)

    if lock_acquired:
        print(f"\nClient-{client_id}: Lock acquired! Lock ID: {lock_id}")
        time.sleep(3)  # Simulate some work
        redlock.release_lock(resource, lock_id)
        print(f"\nClient-{client_id}: Lock released!")
    else:
        print(f"\nClient-{client_id}: Failed to acquire lock.")

if __name__ == "__main__":
    # Define Redis node addresses (host, port)
    redis_nodes = [
        ("localhost", 63791),
        ("localhost", 63792),
        ("localhost", 63793),
        ("localhost", 63794),
        ("localhost", 63795),
    ]

    resource = "shared_resource"
    ttl = 5000  # Lock TTL in milliseconds (5 seconds)

    # Number of client processes
    num_clients = 5

    # Start multiple client processes
    processes = []
    for i in range(num_clients):
        process = multiprocessing.Process(target=client_process, args=(redis_nodes, resource, ttl, i))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()
