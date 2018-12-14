from mrjob.job import MRJob
from mrjob.protocol import ReprValueProtocol
import re

LETTER_WORD_RE = re.compile(r"\b([a-zA-Zа-яА-Я]\.[a-zа-я]\.)")

class MRAbbreviation(MRJob):

    OUTPUT_PROTOCOL = ReprValueProtocol

    def mapper(self, _, line):
        for word in LETTER_WORD_RE.findall(line):
            yield word.lower(), 1

    def combiner(self, word, counts):
            yield word, sum(counts)

    def reducer(self, word, counts):
        s = sum(counts)
        if (s > 10):
            yield None, (word, s)

if __name__ == '__main__':
    MRAbbreviation.run()
