import socket
import time

HOST = "127.0.0.1"
PORT = 6379


def main():
    
    with socket.create_server((HOST, PORT)) as server_socket:
        conn, addr = server_socket.accept() # The .accept() method blocks execution and waits for an incoming connection.
        
        with conn:
            print(f"Connected by {addr}")
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                
                if data == b"PING":
                    conn.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
