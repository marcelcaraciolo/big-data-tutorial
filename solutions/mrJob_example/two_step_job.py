from mrjob.job import MRJob

class MRWordCounter(MRJob):
    def get_words(self, key, line):
        for word in line.split():
            yield word, 1

    def sum_words(self, word, occurrences):
        yield word, sum(occurrences)

    def steps(self):
        return [self.mr(self.get_words, self.sum_words),]

if __name__ == '__main__':
    MRWordCounter.run()
