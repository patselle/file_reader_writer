import os
import io
import time

import pandas as pd
import numpy as np

import reader_writer
import reader_writer_utils as utils


def process(data):

    ### Pandas Example
    # df = pd.read_csv(io.StringIO(data), sep=';')
    # buffer = io.StringIO()
    # df.to_csv(buffer, index = False, header=True, sep=';')

    ### Numpy Example
    array = np.genfromtxt(io.StringIO(data), delimiter=';')
    buffer = io.StringIO()
    np.savetxt(buffer, array, delimiter=';')

    return buffer


if __name__ == "__main__":
    args_dict = vars(utils.get_argument_parser())
    rw = reader_writer.reader_writer(process, **args_dict)
    start_time = time.time()
    rw.run()
    print(f'Total ellapsed time: {time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))}')

