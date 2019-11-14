# CLASH Api

This submodule produces the jar for calling CLASH either from commandline or from other programs.

## CLI-JSON Interface

You can enter a json document as input.

### Parsing a Query

If you only supply a SQL query, this query will be parsed and the understood result output.

For example, this query:

```sql
SELECT ps_partkey
FROM partsupp ps, supplier s, nation n
WHERE ps.suppkey = s.suppkey and s.nationkey = n.nationkey
AND n.name = '[NATION]'
```

Will be translated to:

```json
{
  "query": "SELECT ps_partkey ...",
  "baseRelations": ["partsupp", "supplier", "nation"],
  "baseRelationAliases": ["ps", "s", "n"],
  "binaryPredicates": ["ps.suppkey = s.suppkey", "s.nationkey = n.nationkey"],
  "unaryPredicates: ["n.name = '[NATION]'"]
}
```

If there is an error an object with error attribute is produced.

### Optimizing a query

In addition to a query, you can add optimization parameters:

```json
{
    "optimizationParameters": {
      "taskCapacity": 1000000,
      "availableTasks": 100,
      "globalStrategy": { "name": "Flat" },
      "probeOrderOptimizationStrategy": { "name": "LeastSent" },
    }
}
```

Then you get a JSON formatted optimization result
which consists of a cost estimation, intermediate results, and a final physical graph.

### Running a query

If you finally provide a cluster