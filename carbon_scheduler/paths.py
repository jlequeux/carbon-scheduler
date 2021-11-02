"""Fixed paths to files or directories"""

import pathlib

ROOT = pathlib.Path(__file__).absolute().parent
DATA_FOLDER = f'{ROOT}/data'