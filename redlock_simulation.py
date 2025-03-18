import redis
import time
import uuid
import multiprocessing

client_processes_waiting = [0, 5, 6, 10, 12]

class Redlock:
    def __init__(self, redis_nodes):
        """
        Initialize Redlock with a list of Redis node addresses.
        :param redis_nodes: List of (host, port) tuples.
        """
        self.redis_clients = [redis.Redis(host=host, port=port, decode_responses=True) for host, port in redis_nodes]
        self.target = len(self.redis_clients) // 2 + 1
        self.ttl = 5000 
        
    def acquire_lock(self, resource, ttl):
        """
        Try to acquire a distributed lock.
        :param resource: The name of the resource to lock.
        :param ttl: Time-to-live for the lock in milliseconds.
        :return: Tuple (lock_acquired, lock_id).
        """
        lock_id = str(uuid.uuid4())
        end_time = time.time() + (ttl / 1000)
        taken_clients = 0

        for redis_client in self.redis_clients:
            try:
                if redis_client.set(resource, lock_id, nx=True, px=ttl):
                    taken_clients += 1
            except redis.ConnectionError:
                print("Failed to connect to acquire lockk")

        if time.time() <= end_time and taken_clients >= self.target:
            return True, lock_id
        
        self.release_lock(resource, lock_id)
        return False, None

    def release_lock(self, resource, lock_id):
        """
        Release the distributed lock.
        :param resource: The name of the resource to unlock.
        :param lock_id: The unique lock ID to verify ownership.
        """
        for redis_client in self.redis_clients:
            try:
                stored_value = redis_client.get(resource)
                if stored_value and stored_value == lock_id:
                    redis_client.delete(resource)
            except redis.ConnectionError:
                print("Failed to connect to release lock")


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
