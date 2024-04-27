import sys
from math import sqrt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType, IntegerType


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


if __name__ == '__main__':

    if len(sys.argv) != 4:
        print('Usage: spark-submit SparkPrimesPython.py <in> <out> <max_number>', file=sys.stderr)
        sys.exit(2)

    spark = SparkSession.builder.appName('SparkPrimeNumbersPython').getOrCreate()

    divisors = sieve_of_eratosthenes(int(sys.argv[3]))
    broadcast_divisors = spark.sparkContext.broadcast(divisors)
    is_prime_udf = udf(lambda z: is_prime(z, broadcast_divisors.value), BooleanType())

    numbers_str = spark.read.text(sys.argv[1])
    numbers = numbers_str.withColumn("value", col('value').cast(IntegerType()))
    numbers_with_is_prime = numbers.withColumn('isPrime', is_prime_udf(col('value')))
    primes = numbers_with_is_prime.where('isPrime').select('value')
    result = primes.orderBy(col('value').desc()).limit(100)
    result.write.format('csv').mode('overwrite').save(sys.argv[2])
