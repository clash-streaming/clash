#!/bin/bash

source ./__clash_jar_path.sh


CLASH_ARGS="--task-capacity 1500000 \
  --available-tasks 48 \
  --probe-order-optimization-strategy LeastIntermediate \
  --global-strategy Flat \
  --local"

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
         "dbname": "clash-test",
         "dbuser": "clash",
         "dbpassword": "clash",
         "query": "SELECT * FROM customer LIMIT 100",
         "millisDelay": 1
      },
      "orders": {
         "type": "postgres",
         "dbname": "clash-test",
         "dbuser": "clash",
         "dbpassword": "clash",
         "query": "SELECT * FROM orders LIMIT 100",
         "millisDelay": 1
      },
      "lineitem": {
         "type": "postgres",
         "dbname": "clash-test",
         "dbuser": "clash",
         "dbpassword": "clash",
         "query": "SELECT * FROM  lineitem LIMIT 100",
         "millisDelay": 1
      }
   },
   "sink": {
     "type": "null"
   }
  }'