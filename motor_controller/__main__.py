from __future__ import annotations

import argparse
from typing import Optional


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="AutoCam motor controller entrypoint.")
    parser.add_argument(
        "--manual",
        action="store_true",
        help="Run interactive manual jog/limit calibration mode instead of automatic tracking.",
    )
    parser.add_argument(
        "--driver-ramp",
        action="store_true",
        help="Read or update the BLD-305S driver acceleration/deceleration parameters.",
    )
    args, remaining = parser.parse_known_args(argv)
    if args.manual:
        from .manual import main as manual_main

        return manual_main(remaining)
    if args.driver_ramp:
        from .driver_ramp import main as driver_ramp_main

        return driver_ramp_main(remaining)

    from .main import main as controller_main

    controller_main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
