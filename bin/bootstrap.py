#! /usr/bin/env python3.6

import logging.config
import os
import sys
from pprint import pprint
from typing import List, Dict, Any

import yaml
from robot import run_cli

CONFIG_DIRECTORY = "../config"


def load_yaml(yaml_file_path) -> Dict[str, Any]:
    with open(yaml_file_path, 'r') as f:
        return yaml.safe_load(f.read())


class Bootstrap:

    def __init__(self, config: Dict[str, Any], args: List[str]) -> None:

        self._logger = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.filesystem_config = config['filesystem']
        self.args = args
        print(self.config)
        print(self.filesystem_config)

    def _process_args(self) -> List[str]:

        robot_args = ["--outputdir", os.path.abspath(self.filesystem_config['reports'])]

        for name, path in self.filesystem_config.items():
            robot_args += ["--variable", f"{name.upper()}_DIR:{os.path.abspath(path)}"]

        if len(self.args) == 0:
            robot_args.append(os.path.abspath(self.filesystem_config['features']))
        else:
            robot_args += self.args

        # pprint(f"Robot Args: {robot_args}")
        return robot_args

    def run(self, sys_exit: bool = False) -> int:
        return run_cli(self._process_args(), exit=sys_exit)


if __name__ == "__main__":

    app_config_root_path = os.path.join(os.path.dirname(__file__), CONFIG_DIRECTORY)

    logging.config.dictConfig(load_yaml(os.path.abspath(os.path.join(app_config_root_path, "logging.yaml"))))

    if "APPLICATION_CONFIG" in os.environ:
        application_config_path = os.path.join(app_config_root_path, os.environ["APPLICATION_CONFIG"])
    else:
        application_config_path = os.path.join(app_config_root_path, "local-dev.yaml")

    logging.info(f"Application Config: {application_config_path}")

    application_config = load_yaml(application_config_path)

    print(f"{application_config['application_name']} started..")

    bootstrap = Bootstrap(config=application_config, args=sys.argv[1:])

    try:
        rc = bootstrap.run()
    except Exception as e:
        print(f"Exception occurred: {e}")
        sys.exit(1)

    sys.exit(rc)
