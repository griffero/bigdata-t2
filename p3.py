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
            # movie_id, ["user", user_id]
            # business_id, ["review", user_id]
            yield user_data["business_id"], ["review", user_data["user_id"]]
        elif user_data.get("business_id"):
            # movie_id, ["movie", categories]
            # business_id, ["business", categories]
            yield user_data["business_id"],["business", user_data["categories"]]

    def reducer_join(self, key, values):
        movie_list = list(values)
        movie_category_user = []
        for element in movie_list:
            if element[0] == "business":
                movie_category_user.append(element[1])
            else:
                movie_category_user.append(element)
        if len(movie_category_user) > 1:
            yield key, movie_category_user

    def user_category_map(self, key, value):
        categories = value[0]
        for element in value:
            if element[0] == "review":
                for category in categories:
                    yield element[1], category


    def user_category_reducer(self,key, value):
        category_list = list(value)
        _dict = {i:category_list.count(i) for i in category_list}
        for element in _dict.items():
            yield element[0], [key, element[1]]


    def get_max_reducer(self,key, value):
        max_category_list = list(value)
        yield key, max(max_category_list, key=lambda item: item[1])

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
