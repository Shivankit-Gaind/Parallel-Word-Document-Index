# Parallel-Word-Document-Index

An Inverted Index built using Map-Reduce Architecture which creates local inverted index for a set of documents on each node in a cluster and then merge them to form a distributed global index. MPI programming model was used and performance was measured on a 8-node cluster (each node with 4 cores) with a corpus of around 6.5 GB.
