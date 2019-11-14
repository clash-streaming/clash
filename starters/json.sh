#!/bin/bash

source ./__clash_jar_path.sh

java -jar --add-opens=java.base/java.nio=ALL-UNNAMED $CLASH_JAR \
json -- $'
{
  "query": "SELECT l.orderkey, o.orderdate, o.shippriority
            FROM customer c, orders o, lineitem l
            WHERE c.custkey = o.custkey AND l.orderkey = o.orderkey
              AND c.mktsegment = \'HOUSEHOLD\' AND o.orderdate < date \'1995-03-20\'
              AND l.shipdate > date \'1995-03-20\'",
  "dataCharacteristics": {
      "rates": {
        "c": 150000.0,
        "o": 1500000.0,
        "l": 6001215.0
      },
      "selectivities": {
        "c": { "o": 0.0000066667 },
        "o": { "l": 0.0000006667 }
      }
    },
    "optimizationParameters": {
      "taskCapacity": 1000000,
      "availableTasks": 100,
      "globalStrategy": { "name": "Flat" },
      "probeOrderOptimizationStrategy": { "name": "LeastSent" },
    },
  "cluster": {
    "type": "local",
    "sources": {
      "c": {
        "type": "tpch-file",
        "path": "/Users/manuel/research/datasets/tpc-h/sf1/customer.tbl",
        "attributes": ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"],
        "millisDelay": 1
      },
      "o": {
        "type": "tpch-file",
        "path": "/Users/manuel/research/datasets/tpc-h/sf1/order.tbl",
        "attributes": ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment"],
        "millisDelay": 1
      },
      "l": {
        "type": "tpch-file",
        "path": "/Users/manuel/research/datasets/tpc-h/sf1/lineitem.tbl",
        "attributes": ["orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice", "discount", "tax", "returnflag", "linestatus", "shipdate", "commitdate", "receiptdate", "shipinstruct", "shipmode", "comment"],
        "millisDelay": 1
      }
    },
    "sink": {
      "type": "null"
    }
  }
}
'


