import asyncio
import binascii
import datetime
import random
import re
import string
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
        self.replicas: set[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = set()
        self.role = "master" if not replicaof else "slave"
        super().__init__()

    async def handshake(self) -> None:
        reader, writer = self.open_connection(self.master_host, self.master_port)

        writer.write(str2array("PING"))
        await writer.drain()
        data = await reader.read(100)
        assert data == str2simple_string("PONG"), "Handshake failed"

        writer.write(str2array("REPLCONF", "listening-port", str(self.port)))
        await writer.drain()
        data = await reader.read(100)
        assert data == str2simple_string("OK"), "Handshake failed"

        writer.write(str2array("REPLCONF", "capa", "npsync2"))
        await writer.drain()
        data = await reader.read(100)
        assert data == str2simple_string("OK"), "Handshake failed"

        # initiate a full resynchronization for the first time
        # with unknown replication ID and no offset
        writer.write(str2array("PSYNC", "?", "-1"))
        await writer.drain()
        data = await reader.read(100)
        assert data.startswith(b"+FULLRESYNC"), "Handshake failed"

    async def update_replicas(self, command: list[str]) -> None:
        for _, writer in self.replicas:
            writer.write(str2array(*command))
            await writer.drain()

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
                    writer.write(str2simple_string("PONG"))

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

                case "replconf", *args:
                    if args[0] == "listening-port":
                        self.replicas.add((reader, writer))
                    writer.write(str2simple_string("OK"))

                case "psync", *args:
                    writer.write(
                        str2simple_string(
                            "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"
                        )
                    )

                    writer.write(empty_rdb_file())

            await writer.drain()
            if command[0] in ["set", "del"]:
                await self.update_replicas(command)

        writer.close()
        await writer.wait_closed()

    async def open_connection(
        host: str, port: int
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection(host=host, port=port)

        return reader, writer


def parse_command(message: bytes) -> list[str]:
    """Parse the RESP message and return the command.

    Args:
        message (bytes): The message received from the client.

    Returns:
        list[str]: The command sent by the client.

    Examples:
        >>> parse_command(b"*2\\r\\n$4\\r\\nLLEN\\r\\n$6\\r\\nmylist\\r\\n")
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


def str2simple_string(data: str | None) -> bytes:
    """Create a RESP simple string.

    Description:
        RESP encodes simple strings in the following way:
        +<data>\r\n

    Args:
        data (str | None): The string to be converted to a RESP simple string.
            Can be None to represent a null simple string.

    Returns:
        bytes: The RESP simple string.

    Examples:
        >>> str2simple_string("PONG")
        b"+PONG\\r\\n"
    """
    return f"+{data}\r\n".encode()


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
        >>> str2bulk("mylist")
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
        >>> str2array("LLEN", "mylist")
        b"*2\\r\\n$4\\r\\nLLEN\\r\\n$6\\r\\nmylist\\r\\n"
    """
    if len(data) == 1 and data[0] is None:
        return b"*-1\r\n"

    data_as_resp = "".join(str2bulk(d).decode() for d in data)

    return f"*{len(data)}\r\n{data_as_resp}".encode()


def empty_rdb_file() -> bytes:
    """Create an empty RDB file.

    Returns:
        bytes: The content of an empty RDB file.

    """
    data = binascii.unhexlify(
        "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
    )

    return f"${len(data)}\r\n".encode() + data


if __name__ == "__main__":
    print(parse_command(b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"))
    print(str2simple_string("PONG"))
    print(str2bulk("mylist"))
    print(str2array("LLEN", "mylist"))
