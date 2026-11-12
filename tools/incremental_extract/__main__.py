# flake8: noqa: T201
"""Entry point: python -m tools.incremental_extract {run|benchmark|verify}"""

import sys

from tools.incremental_extract import benchmark, pipeline, verify


def main() -> int:
    commands = ("run", "benchmark", "verify")
    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print("usage: python -m tools.incremental_extract {%s} [args]" % "|".join(commands))
        return 2
    command, argv = sys.argv[1], sys.argv[2:]
    if command == "verify":
        return verify.main()
    if command == "benchmark":
        return benchmark.main(argv)
    return pipeline.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
