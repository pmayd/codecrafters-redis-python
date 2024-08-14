import asyncio
import datetime
import re
from collections import UserDict
from typing import NamedTuple


class Record(NamedTuple):
    value: str
    expires_at: datetime.datetime | None


class RedisServer(UserDict):
    # RESP (REdis Serialization Protocol) is the protocol used by Redis to send responses to clients.
    def __init__(self, host: str, port: int, replicaof: str) -> None:
        self.host = host
        self.port = port
        self.master_host = replicaof.split()[0] if replicaof else ""
        self.master_port = replicaof.split()[1] if replicaof else 0
        self.role = "master" if not replicaof else "slave"
        super().__init__()

    async def run(self) -> None:
        server = await asyncio.start_server(
            self.handle_client, host=self.host, port=self.port, reuse_port=True
        )

        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        print(f"Serving on {addrs}")

        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        while True:
            data = await reader.read(100)
            print("data received: ", data)

            if not data:
                break

            command = parse_command(data)
            match command:
                case ["ping"]:
                    writer.write(b"+PONG\r\n")
                case "echo", arg:
                    writer.write(str2bulk(arg))
                case "set", key, value, "px", ttl:
                    if key not in self.data:
                        ttl = int(ttl)
                        expires_at = datetime.datetime.now() + datetime.timedelta(
                            milliseconds=ttl
                        )
                        self.data[key] = Record(value, expires_at)
                        writer.write(str2bulk("OK"))
                case "set", key, value:
                    if key not in self.data:
                        self.data[key] = Record(value, None)
                        writer.write(str2bulk("OK"))
                case "get", key:
                    value, ttl = self.data.get(key)
                    if ttl is not None and ttl < datetime.datetime.now():
                        del self.data[key]
                        value = None
                    writer.write(str2bulk(value))
                case "info", section:
                    if section == "replication":
                        writer.write(str2bulk(f"role:{self.role}"))

        writer.close()
        await writer.wait_closed()


def parse_command(message: bytes) -> list[str]:
    """Parse the RESP message and return the command.

    Args:
        message (bytes): The message received from the client.

    Returns:
        list[str]: The command sent by the client.

    Examples:
        >>> redis = Redis()
        >>> redis.parse_command(b"*2\\r\\n$4\\r\\nLLEN\\r\\n$6\\r\\nmylist\\r\\n")
        ['LLEN', 'mylist']
    """
    message = message.decode()
    assert message[0] == "*", "Invalid message format"

    parts = re.split("\\r\\n", message)
    number_of_parts = int(parts[0][1:])

    command = []
    for part in parts[1:]:
        if not part or part[0] == "$":
            continue
        command.append(part)

    command[0] = command[0].lower()

    assert len(command) == number_of_parts, "Invalid message format"

    return command


def str2bulk(string: str | None) -> bytes:
    """Create a RESP bulk string.

    Args:
        string (str): The string to be converted to a RESP bulk string.

    Returns:
        bytes: The RESP bulk string.

    Examples:
        >>> redis = Redis()
        >>> redis.create_bulk_string("mylist")
        b"$6\\r\\nmylist\\r\\n"
    """
    if string is None:
        return b"$-1\r\n"

    return f"${len(string)}\r\n{string}\r\n".encode()


if __name__ == "__main__":
    print(parse_command(b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"))
    print(str2bulk("mylist"))
