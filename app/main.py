import asyncio

HOST = "localhost"
PORT = 6379


async def handle_client(reader, writer):
    while True:
        data = await reader.read(100)
        print("data received: ", data)
        message = data.decode()

        if not data:
            break

        if "PING" in message:
            writer.write(b"+PONG\r\n")
            await writer.drain()

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
