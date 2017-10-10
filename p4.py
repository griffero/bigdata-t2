from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
from csv import reader
import math
BUSINESS_HASH = 0
REVIEW_STARS = 1

class UsersCount(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def map_users_with_busines(self, _, line):
        business = line['business_id']
        user = line['user_id']
        # Normalize stars.
        stars = float(line['stars'])/5
        yield user, [business, stars]

    def reduce_users_business(self, user, business):
        business_list = list(business)
        sum_of_stars_per_user = 0
        for business in business_list:
            sum_of_stars_per_user += business[REVIEW_STARS]**2
        for business in business_list:
            # business, [[user,total_sum], star]
            yield business[BUSINESS_HASH], [[user, sum_of_stars_per_user], business[REVIEW_STARS]]

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
                mapper=self.map_users_with_busines,
                reducer=self.reduce_users_business
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
