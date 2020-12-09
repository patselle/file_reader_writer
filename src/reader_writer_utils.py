import os
import fnmatch
import argparse
from pathlib import Path
import shutil
import multiprocessing as mp

class readable_dir(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        prospective_dir = values
        if not os.path.isdir(prospective_dir):
            raise argparse.ArgumentTypeError(f"readable_dir:{prospective_dir} is not a valid path")
        if os.access(prospective_dir, os.R_OK):
            setattr(namespace, self.dest,  prospective_dir)
        else:
            raise argparse.ArgumentTypeError(f"readable_dir:{prospective_dir} is not a readable path")


class writeable_dir(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        prospective_dir = values
        if not os.path.isdir(prospective_dir):
            raise argparse.ArgumentTypeError(f"writeable_dir:{prospective_dir} is not a valid path")
        if os.access(prospective_dir, os.W_OK):
            setattr(namespace, self.dest,  prospective_dir)
        else:
            raise argparse.ArgumentTypeError(f"writeable_dir:{prospective_dir} is not a writeable path")


class is_valid_file(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        prospective_file = values
        if not os.path.exists(prospective_file):
            raise argparse.ArgumentTypeError(f"execution file:{prospective_file} does not exists")
        else:
            #return open(arg, 'r')  # return an open file handle
            setattr(namespace, self.dest,  prospective_file)



def get_argument_parser():
    parser = argparse.ArgumentParser(description='main')

arser.add_argument('--input_root', '-i', action=readable_dir, default=os.path.join(os.getcwd(), 'input'), help='Path to root dir of source files')
    parser.add_argument('--output_root', '-o', action=writeable_dir, default=os.path.join(os.getcwd(), 'output'), help='Path to root dir of output files')
    parser.add_argument('--processed_root', action=is_valid_file, default=os.path.join(os.getcwd(), 'tmp'), help='Path to processing file, default: os.getced()/tmp')
    parser.add_argument('--show_progress', type=bool, default=True, help='Show progress every 10 seconds, default: True')
    parser.add_argument('--thread_limit', type=int, default=5, help='Specify how many items in memory are needed to start an additional process, default: 5')
    parser.add_argument('--max_cpu_count', type=int, default=mp.cpu_count(), help='Specify how many logical cores can be used for processing, default: cpu_count()')
    parser.add_argument('--max_queue_size', type=int, default=10, help='Specify maximum items can be pushed into the memory, default: 10')
    parser.add_argument('--clean_working_dir', type=bool, default=False, help='Clean working dir(proccessed files) after completation of the programm, default: False')
    parser.add_argument('--batch_size', type=int, default=1, help='Specify batch size. Here only used to define interval sequence in which pandas data frame objects are saved')

    return parser.parse_args()



def __append_zeros(val, length):
	if len(val) == 2:
		val += '.'
	while len(val) < length:
		val += '0'
	return val


def __percentage(x, all):
	return int(float(x)/all * 10000) / 100.0


def iterate_files(dirpath, pattern = None):
	if pattern is None:
		pattern = '*'

	for root, dirs, files in os.walk(dirpath, topdown=False):
		for file in files:
			if fnmatch.fnmatch(file, pattern):
				yield os.path.relpath(os.path.join(root, file), dirpath)


def ensure_directory(dirpath):
	if not os.path.isdir(dirpath):
		os.mkdir(dirpath)


def clean_dir(dirpath):
    dirpath = Path(dirpath)
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)