import os
import sys

def getAlignedSequence(raw_sequence,match_string):
    """
    Parses a CIGAR string and return a list of aligned sequences
    """

    num = []
    matches_and_snips = []

    for char in match_string:

        if char not in ["S","M","s","m","N","n","D","d","I","i"]:
            num.append(char)
        else:
            length = int("".join(num))

            matches_and_snips.append((length,char))

            num = []

    # Using that information construct the list of sequences.
    # note: Sam starts on 1 and python starts on 0

    start = 0

    sequences = []

    for m in matches_and_snips:

        length_of_match = m[0] 
        type_of_match   = m[1]

        if type_of_match == "M":
            sequences.append(raw_sequence[start:start + length_of_match])

        start += length_of_match

    return sequences

if __name__=="__main__":
	print getAlignedSequence("AGAAGTTCCAAGTTGAGCT","10M15S")
