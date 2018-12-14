from mrjob.job import MRJob
from mrjob.protocol import ReprValueProtocol
import re

LETTER_WORD_RE = re.compile(r"\b[A-Za-zА-Яа-я]+\b")

class MRAllNames(MRJob):

    OUTPUT_PROTOCOL = ReprValueProtocol

    def mapper(self, _, line):
        for word in LETTER_WORD_RE.findall(line):
            c = word[0]
            yield word.lower(), 1 if c.isupper() else -1

    def combiner(self, word, counts):
        i = 0
        sum = 0
        for c in counts:
            i+=1
            sum+=c
        if (i > 10 and sum > 0):
            yield word, i

    def reducer(self, word, counts):
        yield None, (word, sum(counts))

if __name__ == '__main__':
    MRAllNames.run()
