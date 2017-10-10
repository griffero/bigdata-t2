from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools

class UsersCount(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol
    def mapper_userid_business(self, _, line):
        user = line['user_id']
        business = line['business_id']
        yield user, business

    def reducer1(self, key, values):
        business_list = list(values)
        yield [key,len(business_list)], business_list

    def map2(self, key, values):
        for business in values:
            yield  business ,key

    def reducer2(self, key, values):
        for subset in itertools.combinations(values,2):
            yield [subset[0][0],subset[1][0]] , subset[0][1] + subset[1][1]

    def map3(self, key, value):
        yield [key,value],1

    def reducer3(self, key, values):
        intersection = float(sum(values))
        union = float(key[1])
        jaccard = intersection/(union-intersection)
        if jaccard > 0.5:
            yield 'Best match by Jaccard index',[intersection/(union-intersection),key[0]]

    def steps(self):
        return [
                    MRStep(
                        mapper=self.mapper_userid_business,
                        reducer=self.reducer1
                        ),
                    MRStep(
                        mapper=self.map2,
                        reducer=self.reducer2
                        ),
                    MRStep(
                        mapper=self.map3,
                        reducer=self.reducer3
                        )
                ]

if __name__ == '__main__':
    UsersCount.run()
