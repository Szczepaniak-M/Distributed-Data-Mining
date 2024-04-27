import sys
import time
from math import sqrt

import dask.bag as db
from distributed import Client


def sieve_of_eratosthenes(number):
    max_number = int(sqrt(number)) + 1
    prime_candidates = [i for i in range(3, max_number, 2)]
    prime_numbers = []
    while prime_candidates[0] <= sqrt(max_number):
        prime = prime_candidates.pop(0)
        prime_numbers.append(prime)
        prime_candidates = [i for i in prime_candidates if i % prime != 0]
    prime_numbers.extend(prime_candidates)
    return prime_numbers


def is_prime(number_str, divisors):
    number = int(number_str)
    if number < 2:
        return False
    if number == 2:
        return True
    if not number & 1:
        return False
    for divisor in divisors:
        if number > divisor and number % divisor == 0:
            return False
    return True


def main(args):
    input_path, output_path = args
    hdfs = 'hdfs://master:9000'
    hdfs_input_path = hdfs + input_path
    file = db.read_text(hdfs_input_path, blocksize='128MB')
    numbers = file.map(lambda x: int(x))
    primes = numbers.filter(lambda x: is_prime(x, divisors)).topk(100)
    primes_str = primes.map(lambda x: str(x))
    result = primes_str.to_textfiles(hdfs + output_path + '*.txt')
    return result


# Hadoop File System
client = Client('PRIVATE_IP:8786')

input_path = sys.argv[1]
output_path = sys.argv[2]
max_number = int(sys.argv[3])

start_time = time.time()

divisors = sieve_of_eratosthenes(max_number)
futures = client.submit(main, (input_path, output_path))
print(futures.result())

end_time = time.time()
print(f"Time: {end_time - start_time} seconds")
