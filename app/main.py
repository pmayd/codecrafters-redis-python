import asyncio
import argparse

from app.redis import RedisServer

HOST = "localhost"
DEFAULT_PORT = 6379


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", "-p", type=int, default=DEFAULT_PORT)
    parser.add_argument("--replicaof", type=str, default="")

    return parser.parse_args()


async def main(server: RedisServer) -> None:
    await asyncio.gather(server.handshake(), server.run())


if __name__ == "__main__":
    args = parse_args()
    server = RedisServer(host=HOST, port=args.port, replicaof=args.replicaof)

    asyncio.run(main(server))
