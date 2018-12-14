from mrjob.job import MRJob
from mrjob.step import MRStep
import re

LATIN_WORD_RE = re.compile(r"\b[a-zA-Z]+\b")

class MRMaxLatin(MRJob):

    def mapper(self, _, line):
        for word in LATIN_WORD_RE.findall(line):
            yield word.lower(), 1

    def combiner(self, word, counts):
        yield (word, sum(counts))

    def reducer(self, word, counts):
        yield None, (sum(counts), word)

    def reducer2(self, _, pairs):
        yield max(pairs)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer2)
        ]

if __name__ == '__main__':
    MRMaxLatin.run()
