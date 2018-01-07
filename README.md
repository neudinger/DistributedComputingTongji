# DistributedComputingTongji
Academic Year 2017-2018 Semester 1 2170042 - Distributed Computing Assignment B

`pip install -r requirements.txt`

dask-scheduler distributed: see [dask-scheduler](https://github.com/dask/distributed.git)
- run before as background
```
dask-scheduler
```


```
./main.py "file.txt" "number of (local or remote) cluster"
```

dask-scheduler used for distributed computing

Requirement
Each team of candidates is required to design and implement a distributed system, which supports
distributed file storage and distributed computation. The system consists of at least 4 nodes, where
each node could be either a physical node or a virtual node.
## Task A
In terms of distributed file storage, the following functionalities and features should be supported:
1. Uploading and downloading of files. Files can be uploaded and downloaded at each node.
2. Partitioning and replication of files. Large-size files should be divided into partitions for
storage in the system, and there must be replicas for each file. Even if there is a failure of up
to 20% nodes, the entire system can continue to provide access to all stored files.
3.
Consistency maintenance of files. Whenever a file is being updated at a node, all replicas of
the file are updated accordingly.

## Task B

Computational resources on distributed nodes are collectively utilized for processing a set of data
records 1 (see attachment) provided by a mobile network operator. The following statistics should
be derived and outputted as plaintext files or Excel documents:
***
1. The average amount of outgoing calls every day by each mobile customer.
The data fields (columns) are described in Appendix A.
Page 1 of 4
Academic Year 2017-2018 Semester 1 / 2170042 - Distributed Computing / Assignment B
Output record format: <calling number, amount of calls>
2. Proportions of the network operators (China Mobile/China Unicom/China Telecom) of the
called parties by each type of call (local/long-distance/roaming).
3. Proportions of call durations in different time sections 2 by each mobile customer.
Output record format: <calling number, proportion of call duration in TS1, proportion of call
duration in TS2, ..., proportion of call duration in TS8>

