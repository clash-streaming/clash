#!/bin/bash

source ./__clash_jar_path.sh


CLASH_ARGS="--task-capacity 1500000 \
  --available-tasks 48 \
  --probe-order-optimization-strategy LeastIntermediate \
  --partitioning-attributes-selection-strategy None \
  --global-strategy Flat \
  --nimbus dbis-expsrv1 \
  --config dbis-expsrv1.yaml"

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
         "type": "postgres",
         "query": "SELECT * FROM customer ORDER BY c_custkey LIMIT 100",
         "millisDelay": 10
      },
      "orders": {
         "type": "postgres",
         "query": "SELECT * FROM orders ORDER BY o_orderkey, o_custkey LIMIT 100",
         "millisDelay": 10
      },
      "lineitem": {
         "type": "postgres",
         "query": "SELECT * FROM lineitem ORDER BY l_orderkey LIMIT 100",
         "millisDelay": 10
      }
   },
   "sink": {
     "type": "null"
   }
  }'