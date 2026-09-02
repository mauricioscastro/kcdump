#!/usr/bin/env python3
import os
import signal
import subprocess
import sys

def main():
    dump_and_load = os.environ.get("DUMP_AND_LOAD", "false").lower() == "true"
    children = []

    http_proc = subprocess.Popen(
        ["miniserve", "--quiet", "--hide-version-footer", "--hide-theme-selector", "-p", "8000", "-t", "kcdump files", "/tmp/kcdump"],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    children.append(http_proc)

    if dump_and_load:
        kcdump_proc = subprocess.Popen(
            ["kcdump"],
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        children.append(kcdump_proc)

    postgres_proc = subprocess.Popen(
        ["postgres"],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    children.append(postgres_proc)

    def shutdown(signum, frame):
        for proc in reversed(children):
            if proc.poll() is None:
                proc.terminate()
        for proc in children:
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    postgres_proc.wait()

    for proc in children:
        if proc.poll() is None:
            proc.terminate()

if __name__ == "__main__":
    main()
