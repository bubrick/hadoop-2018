from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"\w+")

class MRWordMaxLen(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield None, (len(word), word)

    def reducer(self, _, pair):
        yield max(pair)


if __name__ == '__main__':
    MRWordMaxLen.run()
