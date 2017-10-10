from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import time

class UsersCount(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def map_user_business(self, _, line):
        user = line['user_id']
        business = line['business_id']
        yield user, business

    def reduce_business_per_user(self, key, values):
        business_list = list(values)
        yield [key, len(business_list)], business_list

    def map_business_with_user(self, key, values):
        for business in values:
            yield  business, key

    def reduce_users_into_pairs(self, key, values):
        for subset in itertools.combinations(values,2):
            yield [subset[0][0],subset[1][0]] , subset[0][1] + subset[1][1]

    def map_to_count(self, key, value):
        yield [key,value], 1

    def reduce_and_get_jaccard(self, key, values):
        intersection = float(sum(values))
        union = float(key[1])
        jaccard = float(intersection/(union-intersection))
        if jaccard > 0.5:
            yield 'Best match by Jaccard index', [intersection/(union-intersection), key[0]]

    def steps(self):
        return [
                    MRStep(
                        mapper=self.map_user_business,
                        reducer=self.reduce_business_per_user
                        ),
                    MRStep(
                        mapper=self.map_business_with_user,
                        reducer=self.reduce_users_into_pairs
                        ),
                    MRStep(
                        mapper=self.map_to_count,
                        reducer=self.reduce_and_get_jaccard
                        )
                ]

if __name__ == '__main__':
    print "Begin..."
    time_init = time.time()
    UsersCount.run()
    duration = time.time() - time_init
    print "End!"
    print "________________________________"
    print "Query duration: {0}".format(duration)
