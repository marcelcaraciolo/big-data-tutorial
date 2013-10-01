from mrjob.job import MRJob
from mrjob import protocol
import re


WORD_RE = re.compile(r"[\w']+")


class MRWordFreqCount(MRJob):
    OUTPUT_PROTOCOL = protocol.JSONProtocol

    def steps(self):
        return [self.mr(self.mapper, self.reducer)]

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    MRWordFreqCount.run()