# word_count.py
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# Regular expression to find words (sequences of letters)
WORD_RE = re.compile(
    r"[\w']+"
)  # Matches sequences of alphanumeric characters and apostrophes


class MRWordCount(MRJob):

    def steps(self):
        """Define the sequence of MapReduce steps."""
        return [
            MRStep(
                mapper=self.mapper_get_words,
                combiner=self.combiner_count_words,  # Optional optimization
                reducer=self.reducer_count_words,
            )
        ]

    def mapper_get_words(self, _, line):
        """
        Mapper: Reads lines, yields each word with a count of 1.
        Input: key=None (ignored, represented by _), value=one line of text.
        Output: Yields (word, 1) pairs.
        """
        # Use the regular expression to find all words in the line
        for word in WORD_RE.findall(line):
            # Convert word to lowercase for case-insensitive counting
            # Yield the word (as the key) and 1 (as the value)
            yield word.lower(), 1

    def combiner_count_words(self, word, counts):
        """
        Combiner (Optional Optimization): Sums counts for a given word *locally* on the mapper node.
        Reduces the amount of data shuffled across the network.
        Input: key=word, value=iterator of 1s (counts) seen by this mapper for this word.
        Output: Yields (word, partial_sum) pairs.
        """
        # Sum the counts for the word encountered on this mapper node
        yield word, sum(counts)

    def reducer_count_words(self, word, counts):
        """
        Reducer: Sums all counts received for a specific word from all mappers/combiners.
        Input: key=word, value=iterator of counts (either 1s from mappers or partial sums from combiners).
        Output: Yields (word, total_sum) pairs.
        """
        # Sum the counts received for this word from all mappers/combiners
        yield word, sum(counts)


# This makes the script runnable from the command line
if __name__ == "__main__":
    MRWordCount.run()
