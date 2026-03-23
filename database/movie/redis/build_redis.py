import argparse
import shlex
import socket
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Redis commands from UTF-8 script file.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--password", default="")
    parser.add_argument("--script", required=True)
    return parser.parse_args()


def encode_resp(args: list[str]) -> bytes:
    parts: list[bytes] = [f"*{len(args)}\r\n".encode("ascii")]
    for arg in args:
        raw = arg.encode("utf-8")
        parts.append(f"${len(raw)}\r\n".encode("ascii"))
        parts.append(raw + b"\r\n")
    return b"".join(parts)


def read_line(sock_file) -> bytes:
    line = sock_file.readline()
    if not line:
        raise RuntimeError("Redis connection closed unexpectedly")
    if not line.endswith(b"\r\n"):
        raise RuntimeError("Invalid Redis protocol line ending")
    return line[:-2]


def read_resp(sock_file):
    prefix = sock_file.read(1)
    if not prefix:
        raise RuntimeError("Redis connection closed unexpectedly")

    if prefix == b"+":
        return read_line(sock_file).decode("utf-8", errors="replace")
    if prefix == b"-":
        error = read_line(sock_file).decode("utf-8", errors="replace")
        raise RuntimeError(error)
    if prefix == b":":
        return int(read_line(sock_file))
    if prefix == b"$":
        length = int(read_line(sock_file))
        if length == -1:
            return None
        data = sock_file.read(length)
        tail = sock_file.read(2)
        if tail != b"\r\n":
            raise RuntimeError("Invalid bulk string terminator")
        return data
    if prefix == b"*":
        count = int(read_line(sock_file))
        if count == -1:
            return None
        return [read_resp(sock_file) for _ in range(count)]

    raise RuntimeError(f"Unsupported Redis response prefix: {prefix!r}")


def send_command(sock, sock_file, args: list[str]) -> None:
    sock.sendall(encode_resp(args))
    read_resp(sock_file)


def main() -> None:
    args = parse_args()
    script_path = Path(args.script)
    if not script_path.exists():
        raise FileNotFoundError(f"Redis sample data file not found: {script_path}")

    with socket.create_connection((args.host, args.port), timeout=10) as sock:
        sock_file = sock.makefile("rb")

        if args.password:
            send_command(sock, sock_file, ["AUTH", args.password])

        with script_path.open("r", encoding="utf-8") as f:
            for lineno, raw in enumerate(f, start=1):
                line = raw.strip()
                if lineno == 1:
                    line = line.lstrip('\ufeff')
                if not line or line.startswith("#"):
                    continue

                tokens = shlex.split(line, posix=True)
                if not tokens:
                    continue

                try:
                    send_command(sock, sock_file, tokens)
                except Exception as exc:
                    raise RuntimeError(f"Redis command failed at line {lineno}: {line}") from exc

    print("Redis movie keys initialized with UTF-8 sample data in DB 0.")


if __name__ == "__main__":
    main()

