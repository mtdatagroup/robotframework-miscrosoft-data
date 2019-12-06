import logging.config
import os
import sys
from typing import List

import yaml
from dotenv import load_dotenv
from robot import run_cli


class Bootstrap:

    def __init__(self, args: List[str]) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self.args = args

    def run(self, sys_exit: bool = False) -> int:
        return run_cli(self.args, exit=sys_exit)


if __name__ == "__main__":

    load_dotenv()

    logging_config = os.path.join(os.path.dirname(__file__), "config", "logging.yaml")

    with open(logging_config, "rt") as f:
        logging.config.dictConfig(yaml.safe_load(f.read()))

    bootstrap = Bootstrap(sys.argv[1:])

    try:
        rc = bootstrap.run()
    except Exception as e:
        print(e)
        sys.exit(1)

    sys.exit(rc)
