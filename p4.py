from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
from csv import reader
import math

class UsersCount(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol
    def initial_map(self, _, line):
        business = line['business_id']
        users = line['user_id']
        stars = line ['stars']
        yield users, [business, stars]

    def reducer1(self, key, values):
        _values = list(values)
        _sum = 0
        for values in _values:
            _sum += values[1]**2
        # users, [business, stars]
        # yield [key, _sum], _values
        for value in _values:
            # business, [[user,total_sum], star]
            yield value[0], [[key,_sum], value[1]]

    def reducer_multiply(self, key, values):
        for subset in itertools.combinations(values,2):
            yield [ [subset[0][0][0], subset[0][0][1]], [ subset[1][0][0], subset[1][0][1] ]], subset[0][1]*subset[1][1]

    def reducer_sum(self, key, values):
        dividend = float(sum(values))
        divisor = float(math.sqrt(key[0][1])*math.sqrt(key[1][1]))
        similarity = dividend/divisor
        if similarity > 0.5:
            yield [key[0][0],key[1][0]], similarity

    def steps(self):
        return [
            MRStep(
                mapper=self.initial_map,
                reducer=self.reducer1
                ),
            MRStep(
                reducer=self.reducer_multiply
                ),
            MRStep(
                reducer=self.reducer_sum
                ),
            ]

if __name__ == '__main__':
    UsersCount.run()
