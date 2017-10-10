from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
from csv import reader

class UsersCount(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol
    def initial_map(self, _, line):
        user_data = line
        if user_data.get("user_id"):
            score_points = user_data["useful"] + user_data["funny"] + user_data["cool"]
            # business_id, ["review", user_id, total_points]
            yield user_data["business_id"], ["review", user_data["user_id"], score_points ]
        elif user_data.get("business_id"):
            # business_id, ["business", categories]
            yield user_data["business_id"],["business", user_data["categories"]]

    def reducer_join(self, key, values):
        business_list = list(values)
        business_category_review = []
        for element in business_list:
            if element[0] == "business":
                business_category_review.append(element[1])
            else:
                business_category_review.append(element)
        if len(business_category_review) > 1:
            yield key, business_category_review

    def user_category_map(self, key, value):
        categories = value[0]
        for element in value:
            if element[0] == "review":
                for category in categories:
                    yield [element[1], element[2]], category

    def user_category_reducer(self,key, value):
        category_list = list(value)
        _dict = {i:category_list.count(i) for i in category_list}
        for element in _dict.items():
            yield element[0], [key, element[1]]

    def get_max_reducer(self,key, value):
        max_category_list = list(value)
        max_in_category = max(max_category_list, key=lambda item: item[1])
        user_hash = max_in_category[0][0]
        total_votes_in_category = float(max_in_category[0][1])
        total_reviews_in_category = float(max_in_category[1])
        yield key, [user_hash, total_votes_in_category/total_reviews_in_category]

    def steps(self):
        return [
            MRStep(
                mapper=self.initial_map,
                reducer=self.reducer_join
                ),
            MRStep(
                mapper=self.user_category_map,
                reducer=self.user_category_reducer
                ),
            MRStep(
                reducer=self.get_max_reducer
                )
            ]

if __name__ == '__main__':
    UsersCount.run()
