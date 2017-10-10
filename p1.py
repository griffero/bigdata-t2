from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import time

class UsersCount(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def createWordList(self, line):
        wordList2 =[]
        wordList1 = line.split()
        for word in wordList1:
            cleanWord = ""
            for char in word:
                if char in '!,.?":;0123456789':
                    char = ""
                cleanWord += char
            wordList2.append(cleanWord)
        return wordList2

    def mapper_separate_text(self, _, line):
        review = line['review_id']
        text = self.createWordList(line['text'])
        for word in text:
            yield word, review

    def unique_words_reducer(self, key, values):
        reviews = list(values)
        if len(reviews) == 1:
            yield reviews[0], 1

    def count_unique_words_per_review(self, key, values):
        yield ["Max", [key, sum(values)]]

    def get_most_original_review(self, key, values):
        yield key, max(values, key=lambda item: item[1])

    def steps(self):
        return [
                    MRStep(
                        mapper=self.mapper_separate_text,
                        reducer=self.unique_words_reducer
                        ),
                    MRStep(
                        reducer=self.count_unique_words_per_review
                        ),
                    MRStep(
                        reducer=self.get_most_original_review
                        ),
                ]

if __name__ == '__main__':
    print "Begin..."
    time_init = time.time()
    UsersCount.run()
    duration = time.time() - time_init
    print "End!"
    print "________________________________"
    print "Query duration: {0}".format(duration)
