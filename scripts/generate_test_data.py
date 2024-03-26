import getopt
import socket
import sys
import uuid
import json
from faker import Faker

DEFAULT_HOST, DEFAULT_PORT = "localhost", 8090


def main():
    host, port = parse_args()
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        generate_test_data(sock)
    finally:
        sock.close()


def parse_args():
    host, port = DEFAULT_HOST, DEFAULT_PORT

    opts, args = getopt.getopt(sys.argv[1:],"h:p:", ["help", "host=", "port="])
    for opt, arg in opts:
        if opt == "--help":
            print("usage: python generate_test_data.py -h <HOST> -p <PORT>")
            sys.exit()
        elif opt in ("-h", "--host"):
            host = arg
        elif opt in ("-p", "--port"):
            port = int(arg)

    return host, port


def generate_test_data(sock):
    generate_users(sock)


def generate_users(sock):
    sql = "CREATE TABLE users (name TEXT, age INTEGER, job TEXT, email TEXT, phone TEXT);"
    message = message_with_query(sql)
    sock.sendall(bytes(message, encoding="utf-8"))
    recvall(sock)

    fake = Faker('en_US')
    for i in range(10000):
        name = fake.name()
        age = fake.unique.random_int(min=1, max=100000)
        email = fake.email()
        job = fake.job()
        phone = fake.phone_number()
        sql = f"INSERT INTO users (name, age, job, email, phone) VALUES ('{name}', {age}, '{job}', '{email}', '{phone}')"
        

        message = message_with_query(sql)
        sock.sendall(bytes(message, encoding="utf-8"))
        recvall(sock)


def message_with_query(query):
    message = {
        "reqId": str(uuid.uuid4()), 
        "action": "sql_statement", 
        "data": {
            "sql_statement": query.replace('\n', '')
        },
    }
    return json.dumps(message) + "\n"


def recvall(sock):
    BUFF_SIZE = 1024
    data = b''
    while True:
        part = sock.recv(BUFF_SIZE)
        data += part
        if len(part) < BUFF_SIZE:
            break
    return data

main()