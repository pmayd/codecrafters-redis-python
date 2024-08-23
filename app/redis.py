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
    def __init__(self, host: str, port: int, replicaof: list[str]) -> None:
        self.host = host
        self.port = port
        self.master_host = replicaof.split()[0] if replicaof is not None else ""
        self.master_port = int(replicaof.split()[1]) if replicaof is not None else 0
        self.replicas: set[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = set()
        self.role = "master" if not replicaof else "slave"
        super().__init__()
        self.offset = 0

    async def handshake(self) -> None:
        if not self.master_port:
            return

        reader, writer = await open_connection(self.master_host, self.master_port)

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

        await self.handle_client(reader, writer, replica_conn=True)

    async def update_replicas(self, command: list[str]) -> None:
        for _, writer in self.replicas:
            print("updating replicas with command: ", command)
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

    async def handle_client(self, reader, writer, replica_conn: bool = False):
        while True:
            data = await reader.read(100)

            if not data:
                break

            while True:
                try:
                    commands = parse_command(data)
                    break
                except Exception:
                    data += await reader.read(100)
                    print(data)

            print("Received:", data)
            print("Commands:", commands)
            for command, message_length in commands:
                match command:
                    case ["ping"]:
                        response = [str2simple_string("PONG")]

                    case "echo", arg:
                        response = [str2bulk(arg)]

                    case "set", key, value, "px", ttl:
                        if key not in self.data:
                            ttl = int(ttl)
                            expires_at = datetime.datetime.now() + datetime.timedelta(
                                milliseconds=ttl
                            )
                            self.data[key] = Record(value, expires_at)
                            response = [str2bulk("OK")]

                    case "set", key, value:
                        if key not in self.data:
                            self.data[key] = Record(value, None)
                            response = [str2bulk("OK")]

                    case "get", key:
                        value, ttl = self.data.get(key)
                        if ttl is not None and ttl < datetime.datetime.now():
                            del self.data[key]
                            value = None
                        response = [str2bulk(value)]

                    case "info", section:
                        if section == "replication":
                            data = [
                                f"role:{self.role}",
                                f"master_replid:{"".join(random.choices(string.ascii_letters+string.digits, k=40))}",
                                "master_repl_offset:0",
                            ]
                            response = [str2bulk(*data)]

                    case "replconf", *args:
                        if args[0] == "listening-port":
                            self.replicas.add((reader, writer))
                            response = [str2simple_string("OK")]
                        elif args[0] == "getack":
                            response = [str2array("REPLCONF", "ACK", str(self.offset))]

                    case "psync", *args:
                        response = [
                            str2simple_string(
                                "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"
                            ),
                            empty_rdb_file(),
                        ]

                if not replica_conn:
                    for resp in response:
                        writer.write(resp)
                        await writer.drain()

                    if command[0] in ["set", "del"]:
                        await self.update_replicas(command)
                else:
                    if "getack" in command:
                        writer.write(response[0])
                        await writer.drain()

                    self.offset += message_length

        writer.close()
        await writer.wait_closed()


async def open_connection(
    host: str, port: int
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    reader, writer = await asyncio.open_connection(host=host, port=port)

    return reader, writer


def parse_command(message: bytes) -> list[tuple[list[str], int]]:
    """Parse the RESP message and return a list of commands.

    Args:
        message (bytes): The message received from the client.

    Returns:
        list[tuple[list[str], int]]: A list of tuples containing the command and the length of the bytes message.

    Examples:
        >>> parse_command(b"*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n")
        [(['replconf', 'getack', '*'], 37)]
    """
    # throw away all bytes until "*" is found
    message_start = message.find(b"*")
    if message_start != -1:
        message = message[message_start:].decode()
    else:
        if message.startswith(b"+") or message.startswith(b"@"):
            return []
        raise ValueError(
            "Invalid message format. Expected a RESP array or RESP simple string."
        )

    resp_commands = re.split(r"[*](\d+)", message)
    commands = []

    for resp_command in resp_commands:
        if not resp_command:
            continue
        elif resp_command[0].isdigit():
            number_of_parts = int(resp_command[0])
        else:
            parts = re.split("\\r\\n", resp_command)

            command = []
            for part in parts[1:]:
                if not part or part[0] == "$":
                    continue
                command.append(part.lower())

            commands.append(
                (command, len(resp_command) + 1 + len(str(number_of_parts).encode()))
            )

            assert len(command) == number_of_parts, "Invalid message format"

    return commands


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
    print(parse_command(b"*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n"))
    print(str2simple_string("PONG"))
    print(str2bulk("mylist"))
    print(str2array("LLEN", "mylist"))
