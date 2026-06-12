"""Command-line server entry point."""

from __future__ import annotations

import argparse

from .config import Settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="启动无数据库 Python 抽卡引擎服务。")
    parser.add_argument("--host", default=None, help="服务监听地址，例如 127.0.0.1 或 0.0.0.0。")
    parser.add_argument("--port", default=None, type=int, help="服务监听端口，例如 8080。")
    return parser


def run(host: str | None = None, port: int | None = None) -> None:
    import uvicorn

    settings = Settings.from_env()
    bind_host = host or settings.host
    bind_port = port or settings.port

    print(f"服务启动中：http://{bind_host}:{bind_port}/docs")
    uvicorn.run("gacha_engine_service.main:app", host=bind_host, port=bind_port)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    run(host=args.host, port=args.port)

