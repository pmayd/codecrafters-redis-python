import socket
import time

HOST = "localhost"
PORT = 6379


def main():
    
    server_socket = socket.create_server((HOST, PORT), reuse_port=True)
    conn, addr = server_socket.accept() # wait for client
    
    # listen to incoming data from client
    # for each "PING" respond with "PONG"
    while True:
        time.sleep(0.1)
        
        data = conn.recv(1024)
        
        if not data:
            break
    
        conn.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
