import asyncio
from app.redis import Redis

HOST = "localhost"
PORT = 6379


async def handle_client(reader, writer):
    while True:
        data = await reader.read(100)
        print("data received: ", data)

        if not data:
            break

        command = Redis.parse_command(data)
        match command:
            case ["PING"]:
                writer.write(b"+PONG\r\n")
            case "ECHO", arg:
                writer.write(Redis.str2bulk(arg))

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
