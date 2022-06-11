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