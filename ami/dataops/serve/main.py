"""Entry point for the ami-serve extension.

Invoked via the extension manifest as `{python} main.py <args>`.
"""

from __future__ import annotations

import sys

from ami.dataops.serve.cli import main

if __name__ == "__main__":
    sys.exit(main())
