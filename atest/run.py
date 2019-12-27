import sys

from os.path import abspath, dirname, exists, join, normpath
from robot import run_cli, rebot
from robotstatuschecker import process_output

CURRENT_DIR = dirname(abspath(__file__))
OUTPUT_ROOT = join(CURRENT_DIR, 'results')

sys.path.append(join(CURRENT_DIR, '..', 'src'))

COMMON_OPTS = ('--log', 'NONE', '--report', 'NONE')


def atests(*opts):
    run_robot(*opts)
    process_output(join(OUTPUT_ROOT, 'output.xml'))
    return rebot(join(OUTPUT_ROOT, 'output.xml'), outputdir=OUTPUT_ROOT)


def run_robot(*opts):
    try:
        run_cli(['--outputdir', OUTPUT_ROOT] + list(COMMON_OPTS + opts))
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
