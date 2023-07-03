import argparse
import redis
import logging


# Настройка логирования
logging.basicConfig(filename='redis.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


# Создание парсера аргументов
def define_parser():
    parser = argparse.ArgumentParser(description='Execute commands on Redis servers.')
    parser.add_argument('operation', choices=['flushall', 'checkkey'], help='Operation: flushall, checkkey.')
    parser.add_argument('--hosts', nargs='+', type=str, required=True, help='A list of hosts.')
    parser.add_argument('--ports', nargs='+', type=int, required=True, help='A list of ports.')
    parser.add_argument('--passwords', nargs='+',type=str, required=True, help='A list of passwords.')
    return parser


# Функция для выполнения операции checkkey
def check_key(host, port, passwd):
    try:
        r = redis.Redis(host=host, port=port, password=passwd)
        result = r.dbsize()
        logging.info(f"Number of keys in Redis at {host}:{port} is {result}")
        print(f"Number of keys in Redis at {host}:{port} is {result}")
    except redis.RedisError as e:
        logging.error(f"Can't reach Redis server at {host}:{port}. Error: {e}")
        print(f"Can't reach Redis server at {host}:{port}. Error: {e}")


# Функция для выполнения операции flushall
def flush_redis(host, port, passwd):
    try:
        r = redis.Redis(host=host, port=port, password=passwd)
        result = r.flushall()
        if result:
            logging.info(f"Flushall command executed successfully on {host}:{port}")
            print(f"Flushall command executed successfully on {host}:{port}")
        else:
            logging.error(f"Failed to execute flushall command on {host}:{port}")
            print(f"Failed to execute flushall command on {host}:{port}")
    except redis.RedisError as e:
        logging.error(f"An error occurred with host: {host}, port: {port}. Error: {e}")
        print(f"An error occurred with host: {host}, port: {port}. Error: {e}")


def main():
    parser = define_parser()
    args = parser.parse_args()

    # Убедимся, что все списки имеют одинаковую длину
    if not len(args.hosts) == len(args.ports) == len(args.passwords):
        raise ValueError("All lists must have the same length.")
        
    # Итерация по всем хостам
    for host, port, passwd in zip(args.hosts, args.ports, args.passwords):
        if args.operation == 'flushall':
            flush_redis(host, int(port), passwd)
        elif args.operation == 'checkkey':
            check_key(host, int(port), passwd)


if __name__ == "__main__":
    main()