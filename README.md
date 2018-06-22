# Parallel-Word-Document-Index

An Inverted Index built using Map-Reduce Architecture which creates local inverted index for a set of documents on each node in a cluster and then merge them to form a distributed global index. MPI programming model was used and performance was measured on a 8-node cluster (each node with 4 cores) with a corpus of around 6.5 GB.

The problem statement is given as below:

Consider a large collection of documents distributed in the secondary memories (i.e. hard disks) of multiple nodes in a cluster.

a) Design a program that extracts words and their frequency of occurrence from each document and create a word-document index in each node ranked on the frequency: i.e. for each word, a list of documents is associated with it and the list is ordered by decreasing frequency (of occurrence of that word in the document). Each index is local to a node a word is associated with only those documents in that node. Stop words i.e. frequently occurring words (e.g. a, an, the, for, if, to, then, on, etc.) need
not be indexed. Assume that a list of stop words is available. Implement this program using MPI in C.

b) Design a program to merge all indices into one large word-document index ranked by decreasing frequency to be stored in node. Implement this program using MPI in C.

c) Measure the time taken for a) and b) for different numbers and sizes of documents and independently varying the number of processors used. For each input (of a given number of documents of certain sizes), plot a curve of time taken against number of processors used.
