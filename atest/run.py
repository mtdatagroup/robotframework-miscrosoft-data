import sys
import os

from os.path import abspath, dirname, exists, join, normpath
from robot import run_cli, rebot
from robotstatuschecker import process_output

CURRENT_DIR = dirname(abspath(__file__))
OUTPUT_ROOT = join(CURRENT_DIR, 'results')

sys.path.append(join(CURRENT_DIR, '..', 'src'))

COMMON_OPTS = ('--log', 'NONE', '--report', 'NONE')


def atests(*opts):
    os_includes = get_os_includes(os.name)
    python(*(os_includes+opts))
    process_output(join(OUTPUT_ROOT, 'output.xml'))
    return rebot(join(OUTPUT_ROOT, 'output.xml'), outputdir=OUTPUT_ROOT)


def get_os_includes(operating_system):
    if operating_system == 'nt':
        return '--include', 'windows', '--exclude', 'linux'
    return '--include', 'linux', '--exclude', 'windows'


def python(*opts):
    try:
        print(['--outputdir', OUTPUT_ROOT, '--include', 'pybot'] + list(COMMON_OPTS + opts))
        run_cli(['--outputdir', OUTPUT_ROOT, '--include', 'pybot'] + list(COMMON_OPTS + opts))
    except SystemExit:
        pass


if __name__ == '__main__':
    if len(sys.argv) == 1 or '--help' in sys.argv:
        print(__doc__)
        rc = 251
    else:
        rc = atests(*sys.argv[1:])
    print("\nAfter status check there were %s failures." % rc)
    sys.exit(rc)
