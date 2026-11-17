"""示例辅助：申请空闲端口（仅标准库）。"""
import socket


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port
