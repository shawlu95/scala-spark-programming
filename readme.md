# Scala Spark Programming

## Spark Intro

- Spark is built using scala
- Spark provides repl-shell in Scala and Python
- Spark is part of Hadoop ecosystem
- Has built-in cluster manager (Mesos). But Hadoop YARN is most popular
- Spark Core compute engine + HDFS storage + YARN
- Support high-level API: SQL, Streaming, MLlib, Graph
- Run Scala in jupyter notebook (with `toree`)

```bash
# run in standalone mode
spark-shell

# specify number of thread (cpu cores)
spark-shell --master local[2]
```

Advantages over MapReduce

- interactive shell for fast experiment
- data are kept in memory and can be passed to next operator. MapReduce writes to disk at end of each map-reduce task (super-bad ad ML)
- can do stream-processing (MapReduce only does batch)

Getting started
```bash
brew install sbt
sbt clean package
spark-submit \
  --class FlightAnalysis \
  --master "local[2]" \
  target/scala-2.12/scala-spark-programming_2.12-0.1.0-SNAPSHOT.jar
```

Cluster management
* cluster manager: mesos, yarn, or spark standalone
* **driver** uses a spark context to communicate with the spark cluster manager
* phase 1 initial setup: 
  * when program has a processing task e.g. an action such as count() or collect()
  * cluster manager launches Java processes on **executors**, which register themselves onto driver program
* phase 2 job run:
  * an action is received and translated to job
  * **DAGScheduler** breaks down job into stages, and then tasks
  * **TaskSetManager** assigns tasks to executors, which updates back to the driver program 

---

## Resilient Distributed Dataset

### Transformation & Action

- Transform: create a new RDD, which holds metadata aka **lineage** (e.g. filter, split, convert)
- Action: result is requested (e.g. take, collect)
  - get a quick sense: show, first, take
  - stats: count, countByValue
  - reduce (action): accepts two RDD, must return the same type of RDD
  - aggregate (action): accepts two RDD and init value, can return the same type as init value
    - Require two function: how to merge accumulator with rdd, how to merge accumulator
    - aggregate is **curried** function (partial func)
    
### Persistence

- The entire chain of transform is performed at every action
- Useful to manually persist intermediate results (cache)

```scala
rdd.persist()
rdd.unpersist()
```

### HDFS Recap

- Name node is master node, manage metadata of files
- Data nodes actually store data (partitioned into block size)

### Pair RDD

- Each element is key-value tuple
- special op: `keys`, `values`, `mapValues`, `groupByKey`, `cogroup`,
- `reduceByKey` (transform)
- `combineByKey` (transform)
- `countByKey` (action)
- `lookup` (action): map lookup single element
- `collectAsMap` (action): similar to collect (single RDD)
- `join`, `left|right outer join`

### Broadcast

- cache a small variable to be shared by all nodes
- node will lookup the broadcast cariable by key
- immutable

### Accumulator

- shared by all nodes just like broadcast variable
- not immutable, good for tracking metrics
- executors can only write to accumulator, cannot read
- only main/driver program can only read, not write

### Custom Partitioner
- Hash the key of **pair RDD**
- minimize shuffle, which incur expensive network overhead

---
## SNAP Google Web Graph
* Download dataset [here](https://snap.stanford.edu/data/web-Google.html)
* Each iteration, a node equally split its page rank to all referenced nodes
* At the end of each iteration, damp all page rank by 0.85 and add 0.15 to recover the sum