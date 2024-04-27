import sys
import re
import time

import dask.bag as db
from distributed import Client


def main(args):
    input_path, output_path = args
    hdfs = 'hdfs://master:9000'
    hdfs_input_path = hdfs + input_path
    files = db.read_text(hdfs_input_path, blocksize='128MB')
    text_lower = files.map(lambda line: line.lower())
    words = text_lower.map(lambda line: re.sub("[^a-z]", " ", line).split(" ")).flatten()
    non_empty_words = words.filter(lambda record: record != '')
    word_count = non_empty_words.frequencies().map(lambda record: f'{record[0]},{record[1]}')
    result = word_count.to_textfiles(hdfs + output_path + '*.txt')
    return result


# Hadoop File System
client = Client('PRIVATE_IP:8786')

input_path = sys.argv[1]
output_path = sys.argv[2]

start_time = time.time()

futures = client.submit(main, (input_path, output_path))
print(futures.result())

end_time = time.time()
print(f"Time: {end_time - start_time} seconds")
