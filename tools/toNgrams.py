
import sys


path_file = sys.argv[1]
max_ng_order = sys.argv[2]

with open(path_file) as f:
	corpus = f.readlines()


def sent2Ngram(sent,max_ng_order):
	ngram_list=[]
	tokens=sent.strip().split(" ")
	for ng_order in range(1,max_ng_order+1):
		for i in range(0,len(tokens)-ng_order+1):
			ngram = tokens[i:i+ng_order]
			ngram_list.append(" ".join(ngram) )
	return ngram_list



for sentence in corpus:
	ngram_list = sent2Ngram(sentence,3)
	for ngr in ngram_list:
		print(ngr)
