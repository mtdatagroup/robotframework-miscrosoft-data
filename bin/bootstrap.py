#! /usr/bin/env python3.6

import logging.config
import os
import platform
import sys
from typing import List, Dict, Any

import yaml
from robot import run_cli

APP_CONFIG_DIRECTORY = os.path.abspath(os.path.join(os.path.dirname(__file__), "../config"))
PLATFORM_PREFIX = platform.platform().split('-')[0].lower()


def load_yaml(yaml_file_path) -> Dict[str, Any]:
    with open(yaml_file_path, 'r') as f:
        return yaml.safe_load(f.read())


def load_application_config() -> Dict[str, Any]:

    app_config = load_yaml(os.path.join(APP_CONFIG_DIRECTORY, "app_config.yaml"))

    platform_config_path = os.path.join(APP_CONFIG_DIRECTORY, f"app_config_{PLATFORM_PREFIX}.yaml")

    if os.path.exists(platform_config_path):
        app_config.update(load_yaml(platform_config_path))

    return app_config


class Bootstrap:

    def __init__(self, config: Dict[str, Any], args: List[str]) -> None:

        self._logger = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.args = args

    @property
    def robot_options(self) -> Dict[str, str]:
        return self.config.get("robot_options", {})

    @property
    def filesystem_config(self) -> Dict[str, str]:
        return self.config['filesystem']

    @property
    def robot_listeners(self) -> List[str]:
        return self.config.get("robot_listeners", [])

    def _process_user_config(self) -> List[str]:
        pass

    def _process_args(self) -> List[str]:

        robot_args = ["--outputdir", os.path.abspath(self.filesystem_config['reports'])]

        for name, path in self.filesystem_config.items():
            robot_args += ["--variable", f"{name.upper()}_DIR:{os.path.abspath(path)}"]

        for option, value in self.robot_options.items():
            robot_args += [f"--{option}", value]

        for listener in self.robot_listeners:
            robot_args += ["--listener", listener]

        if len(self.args) == 0:
            robot_args.append(os.path.abspath(self.filesystem_config['features']))
        else:
            robot_args += self.args

        return robot_args

    def run(self, sys_exit: bool = False) -> int:
        return run_cli(self._process_args(), exit=sys_exit)


if __name__ == "__main__":

    logging.config.dictConfig(load_yaml(os.path.abspath(os.path.join(APP_CONFIG_DIRECTORY, "logging.yaml"))))

    config = load_application_config()

    print(config)

    print(f"{config['application_name']} started..")

    bootstrap = Bootstrap(config=config, args=sys.argv[1:])

    try:
        rc = bootstrap.run()
    except Exception as e:
        print(f"Exception occurred: {e}")
        sys.exit(1)

    sys.exit(rc)
