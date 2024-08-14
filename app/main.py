import asyncio
import datetime
from typing import NamedTuple

from app.redis import Redis

HOST = "localhost"
PORT = 6379

DB = {}


class Record(NamedTuple):
    value: str
    expires_at: datetime.datetime | None


async def handle_client(reader, writer):
    while True:
        data = await reader.read(100)
        print("data received: ", data)

        if not data:
            break

        command = Redis.parse_command(data)
        match command:
            case ["ping"]:
                writer.write(b"+PONG\r\n")
            case "echo", arg:
                writer.write(Redis.str2bulk(arg))
            case "set", key, value, "px", ttl:
                if key not in DB:
                    ttl = int(ttl)
                    expires_at = datetime.datetime.now() + datetime.timedelta(
                        milliseconds=ttl
                    )
                    DB[key] = Record(value, expires_at)
                    writer.write(Redis.str2bulk("OK"))
            case "set", key, value:
                if key not in DB:
                    DB[key] = Record(value, None)
                    writer.write(Redis.str2bulk("OK"))
            case "get", key:
                value, ttl = DB.get(key)
                if ttl is not None and ttl < datetime.datetime.now():
                    del DB[key]
                    value = None

                writer.write(Redis.str2bulk(value))

    writer.close()
    await writer.wait_closed()


async def main():
    server = await asyncio.start_server(
        handle_client, host=HOST, port=PORT, reuse_port=True
    )

    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Serving on {addrs}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
