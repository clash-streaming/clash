#!/bin/bash

source ../__clash_jar_path.sh


CLASH_ARGS="--task-capacity 1000000 \
  --available-tasks 16 \
  --probe-order-optimization-strategy LeastIntermediate \
  --partitioning-attributes-selection-strategy None \
  --global-strategy Flat \
  --nimbus ec2-3-120-243-115.eu-central-1.compute.amazonaws.com \
  --config aws-cluster.yaml"

java -jar --add-opens=java.base/java.nio=ALL-UNNAMED $CLASH_JAR \
runstorm ${CLASH_ARGS} -- \
  "SELECT l.orderkey, o.orderdate, o.shippriority FROM customer c, orders o, lineitem l
   WHERE c.custkey = o.custkey AND l.orderkey = o.orderkey AND c.mktsegment = 'HOUSEHOLD'
     AND o.orderdate < date '1995-03-20' AND l.shipdate > date '1995-03-20'" \
  '{
   "rates": {
     "c": 150000.0,
     "o": 1500000.0,
     "l": 6001215.0
   },
   "selectivities": {
     "c": { "o": 0.0000066667 },
     "o": { "l": 0.0000006667 }
   }
  }' \
  '{"sources": {
      "customer": {
         "type": "tpch-file",
         "path": "/common/benchmarks/tpch/skewed/zipf_1/sf_1/customer.tbl",
         "attributes": ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"],
         "millisDelay": 120
      },
      "orders": {
         "type": "tpch-file",
         "path": "/common/benchmarks/tpch/skewed/zipf_1/sf_1/order.tbl",
         "attributes": ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment"],
         "millisDelay": 12
      },
      "lineitem": {
         "type": "tpch-file",
         "path": "/common/benchmarks/tpch/skewed/zipf_1/sf_1/lineitem.tbl",
         "attributes": ["orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice", "discount", "tax", "returnflag", "linestatus", "shipdate", "commitdate", "receiptdate", "shipinstruct", "shipmode", "comment"],
         "millisDelay": 2
      }
   },
   "sink": {
     "type": "null"
   }
  }'