import datetime
import re
from typing import Literal, NamedTuple


class Record(NamedTuple):
    value: str
    expires_at: datetime.datetime | None


class Redis:
    # RESP (REdis Serialization Protocol) is the protocol used by Redis to send responses to clients.

    @staticmethod
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

    @staticmethod
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
    redis = Redis()
    print(redis.parse_command(b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"))
    print(redis.str2bulk("mylist"))
