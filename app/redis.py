import asyncio
import datetime
import re
from collections import UserDict
from typing import NamedTuple
import random
import string


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

    async def handshake(self) -> None:
        reader, writer = await asyncio.open_connection(
            host=self.master_host, port=self.master_port
        )

        writer.write(str2array("PING"))

        writer.close()
        await writer.wait_closed()

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
                        data = [
                            f"role:{self.role}",
                            f"master_replid:{"".join(random.choices(string.ascii_letters+string.digits, k=40))}",
                            "master_repl_offset:0",
                        ]
                        writer.write(str2bulk(*data))

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


def str2bulk(*data: list[str | None]) -> bytes:
    """Create a RESP bulk string.

    Description:
        RESP encodes bulk strings in the following way:
        $<length>\r\n<data>\r\n

    Args:
        data (list[str | None]): The strings to be converted to a RESP bulk string.
            Can have a single None element to represent a null bulk string.

    Returns:
        bytes: The RESP bulk string.

    Examples:
        >>> redis = Redis()
        >>> redis.create_bulk_string("mylist")
        b"$6\\r\\nmylist\\r\\n"
    """

    if len(data) == 1 and data[0] is None:
        return b"$-1\r\n"

    data_as_resp = "\r\n".join(data)

    return f"${len(data_as_resp)}\r\n{data_as_resp}\r\n".encode()


def str2array(*data: list[str | None]) -> bytes:
    """Create a RESP array.

    Description:
        RESP encodes arrays in the following way:
        *<length>\r\n<data>\r\n

    Args:
        data (list[str | None]): The strings to be converted to a RESP array.
            Can have a single None element to represent a null array.

    Returns:
        bytes: The RESP array.

    Examples:
        >>> redis = Redis()
        >>> redis.create_array("LLEN", "mylist")
        b"*2\\r\\n$4\\r\\nLLEN\\r\\n$6\\r\\nmylist\\r\\n"
    """
    if len(data) == 1 and data[0] is None:
        return b"*-1\r\n"

    return f"*{len(data)}\r\n{str2bulk(*data).decode()}".encode()


if __name__ == "__main__":
    print(parse_command(b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"))
    print(str2bulk("mylist"))
