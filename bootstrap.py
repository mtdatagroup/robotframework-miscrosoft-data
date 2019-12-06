#! /usr/bin/env python3.6

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

    def _process_args(self) -> List[str]:

        robot_args = ["--outputdir", os.environ["REPORTS_DIR"]]

        if len(self.args) == 0:
            robot_args.append(os.environ["FEATURES_DIR"])
        else:
            robot_args += self.args

        return robot_args

    def run(self, sys_exit: bool = False) -> int:
        return run_cli(self._process_args(), exit=sys_exit)


if __name__ == "__main__":

    load_dotenv()

    logging_config = os.path.join(os.path.dirname(__file__), "config", "logging.yaml")

    print(f"{os.environ['APPLICATION_NAME']} started..")

    with open(logging_config, "rt") as f:
        logging.config.dictConfig(yaml.safe_load(f.read()))

    bootstrap = Bootstrap(sys.argv[1:])

    try:
        rc = bootstrap.run()
    except Exception as e:
        print(e)
        sys.exit(1)

    sys.exit(rc)
