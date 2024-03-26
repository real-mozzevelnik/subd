import getopt
import socket
import sys
import uuid
import json

DEFAULT_HOST, DEFAULT_PORT = "localhost", 8090

def get_n_send_query(sock):
    while True:
        query = input('>')
        if query == 'quit':
            break

        message = {
            "reqId": str(uuid.uuid4()), 
            "action": "sql_statement", 
            "data": {
                "sql_statement": query.replace('\n', '')
            },
        }
        message = json.dumps(message) + "\n"

        sock.sendall(bytes(message, encoding="utf-8"))
        received = sock.recv(1024*1024*1024)
        received = received.decode("utf-8")
        print(json.dumps(json.loads(received), indent=4))

def parse_args():
    host, port = DEFAULT_HOST, DEFAULT_PORT

    opts, args = getopt.getopt(sys.argv[1:],"h:p:", ["help", "host=", "port="])
    for opt, arg in opts:
        if opt == "--help":
            print("usage: python cli.py -h <HOST> -p <PORT>")
            sys.exit()
        elif opt in ("-h", "--host"):
            host = arg
        elif opt in ("-p", "--port"):
            port = int(arg)

    return host, port


def main():
    host, port = parse_args()
    try:
        print('> TYPE "quit" TO EXIT THE CLI.')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        get_n_send_query(sock)
    finally:
        sock.close()


main()