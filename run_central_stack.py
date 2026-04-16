import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import List, Optional
from functools import partial

from env_loader import load_repo_env


REPO_ROOT = Path(__file__).resolve().parent
REPO_NAME = REPO_ROOT.name
SERVER_ROOT = REPO_ROOT.parent

load_repo_env()


@dataclass
class ManagedProcess:
    name: str
    command: List[str]
    env: dict
    process: Optional[subprocess.Popen] = None
    reader_thread: Optional[threading.Thread] = None


class QuietHttpRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args) -> None:
        return


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the central UWB stack in one terminal.",
    )
    parser.add_argument(
        "--http-port",
        type=int,
        default=int(os.getenv("VIS_HTTP_PORT", "8000")),
        help="Port for the local static file server.",
    )
    parser.add_argument(
        "--skip-scheduler",
        action="store_true",
        help="Run the logger and dashboard server without the scheduler.",
    )
    parser.add_argument(
        "--skip-server",
        action="store_true",
        help="Run the logger and scheduler without the HTTP server.",
    )
    parser.add_argument(
        "--skip-motor-controller",
        action="store_true",
        help="Run the central stack without the pan/truck motor controller.",
    )
    parser.add_argument(
        "--verbose-server",
        action="store_true",
        help="Print HTTP access logs for the static file server.",
    )
    return parser


def stream_output(managed: ManagedProcess) -> None:
    assert managed.process is not None
    assert managed.process.stdout is not None
    try:
        for line in managed.process.stdout:
            print(f"[{managed.name}] {line.rstrip()}", flush=True)
    except Exception as exc:
        print(f"[launcher] output reader error name={managed.name} detail={exc}", flush=True)


def start_process(managed: ManagedProcess) -> None:
    managed.process = subprocess.Popen(
        managed.command,
        cwd=str(REPO_ROOT),
        env=managed.env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    managed.reader_thread = threading.Thread(
        target=stream_output,
        args=(managed,),
        daemon=True,
    )
    managed.reader_thread.start()
    print(
        f"[launcher] started name={managed.name} pid={managed.process.pid} "
        f"cmd={' '.join(managed.command)}",
        flush=True,
    )


def stop_process(managed: ManagedProcess) -> None:
    process = managed.process
    if process is None or process.poll() is not None:
        return

    try:
        process.terminate()
    except Exception:
        return


def kill_process(managed: ManagedProcess) -> None:
    process = managed.process
    if process is None or process.poll() is not None:
        return
    try:
        process.kill()
    except Exception:
        return


def wait_for_shutdown(processes: List[ManagedProcess], timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    for managed in processes:
        process = managed.process
        if process is None:
            continue
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            break
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            continue


def terminate_all(processes: List[ManagedProcess]) -> None:
    for managed in processes:
        stop_process(managed)
    wait_for_shutdown(processes, timeout_s=3.0)
    for managed in processes:
        kill_process(managed)


def serve_http(http_port: int, verbose: bool, stop_event: threading.Event) -> None:
    handler_cls = SimpleHTTPRequestHandler if verbose else QuietHttpRequestHandler
    handler = partial(handler_cls, directory=str(SERVER_ROOT))
    server = ThreadingHTTPServer(("0.0.0.0", http_port), handler)
    server.timeout = 0.5
    print(
        f"[server] serving root={SERVER_ROOT} port={http_port} "
        f"verbose={'1' if verbose else '0'}",
        flush=True,
    )
    try:
        while not stop_event.is_set():
            server.handle_request()
    finally:
        server.server_close()


def main() -> int:
    args = build_parser().parse_args()
    python_exe = sys.executable
    base_env = os.environ.copy()

    processes: List[ManagedProcess] = [
        ManagedProcess(
            name="logger",
            command=[python_exe, "pose_logger.py"],
            env=base_env.copy(),
        ),
    ]

    if not args.skip_scheduler:
        processes.append(
            ManagedProcess(
                name="scheduler",
                command=[python_exe, "scheduler.py"],
                env=base_env.copy(),
            )
        )

    if not args.skip_motor_controller:
        motor_env = base_env.copy()
        motor_env.setdefault("MOTOR_ARM_PROMPT", "0")
        processes.append(
            ManagedProcess(
                name="motor",
                command=[python_exe, "-m", "motor_controller"],
                env=motor_env,
            )
        )

    stop_requested = False
    server_stop_event = threading.Event()
    server_thread: Optional[threading.Thread] = None

    def handle_signal(signum, frame) -> None:
        nonlocal stop_requested
        stop_requested = True
        print(f"[launcher] signal={signum} shutting down", flush=True)

    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)

    print("[launcher] central stack starting", flush=True)
    print(f"[launcher] repo={REPO_ROOT}", flush=True)
    if not args.skip_server:
        print(
            f"[launcher] dashboard=http://localhost:{args.http_port}/{REPO_NAME}/dashboard.html",
            flush=True,
        )

    try:
        for managed in processes:
            start_process(managed)
            time.sleep(0.15)

        if not args.skip_server:
            server_thread = threading.Thread(
                target=serve_http,
                args=(args.http_port, args.verbose_server, server_stop_event),
                daemon=True,
            )
            server_thread.start()

        while not stop_requested:
            for managed in processes:
                process = managed.process
                if process is None:
                    continue
                return_code = process.poll()
                if return_code is not None:
                    print(
                        f"[launcher] process exited name={managed.name} rc={return_code}",
                        flush=True,
                    )
                    stop_requested = True
                    break
            time.sleep(0.20)
    finally:
        server_stop_event.set()
        terminate_all(processes)
        if server_thread is not None:
            server_thread.join(timeout=1.0)
        print("[launcher] central stack stopped", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
