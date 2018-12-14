from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"\w+")

class MRMeanWordLen(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield len(word), 1
			
    def combiner(self, length, counts):
        yield length, sum(counts)

    def reducer(self, length, count):
        total=sum(count)
        yield None, (length * total, total)
		
    def reducer2(self, _, pairs):
        total_len=0
        total_count=0
        for p in pairs:
            total_len+=p[0]
            total_count+=p[1]
        yield None, total_len / total_count

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer2)
        ]
            
            

if __name__ == '__main__':
    MRMeanWordLen.run()
