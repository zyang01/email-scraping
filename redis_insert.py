import redis
import sys
import argparse

def insert_lines_to_redis(redis_client, set_name, lines, batch_size):
    """
    Insert lines into Redis set in batches and print progress.
    
    :param redis_client: Redis client object.
    :param set_name: The Redis set name.
    :param lines: List of lines to be inserted.
    :param batch_size: Number of lines to insert in each batch.
    """
    total_lines = len(lines)
    for i in range(0, total_lines, batch_size):
        batch = lines[i:i + batch_size]
        redis_client.sadd(set_name, *batch)
        print(f"Inserted {min(i + batch_size, total_lines)} / {total_lines} lines.")
    
    print(f"Finished inserting {total_lines} lines into Redis set '{set_name}'.")


def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description='Insert lines from a text file into a Redis set.')
    parser.add_argument('file', type=str, help='Path to the input text file.')
    parser.add_argument('set_name', type=str, help='Redis set name.')
    parser.add_argument('--batch_size', type=int, default=10, help='Number of lines to insert per command (default: 10).')
    parser.add_argument('--host', type=str, default='localhost', help='Redis host (default: localhost).')
    parser.add_argument('--port', type=int, default=6379, help='Redis port (default: 6379).')
    
    args = parser.parse_args()
    
    # Initialize Redis client
    redis_client = redis.StrictRedis(host=args.host, port=args.port, db=0, decode_responses=True)
    
    # Read lines from the provided text file
    try:
        with open(args.file, 'r') as file:
            lines = [line.strip() for line in file.readlines()]
    except Exception as e:
        print(f"Error reading file: {e}")
        sys.exit(1)
    
    # Insert lines into Redis set
    insert_lines_to_redis(redis_client, args.set_name, lines, args.batch_size)


if __name__ == "__main__":
    main()
