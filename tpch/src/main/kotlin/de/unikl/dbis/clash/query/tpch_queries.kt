package de.unikl.dbis.clash.query

object TpcHConstants {
    const val part = "part"
    const val supplier = "supplier"
    const val partsupp = "partsupp"
    const val nation = "nation"
    const val region = "region"

    const val partkey = "partkey"
    const val suppkey = "suppkey"
    const val nationkey = "nationkey"
    const val regionkey = "regionkey"
}

object TpcHJoinsAndFilters {
    /**
     * SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
     * FROM part, supplier, partsupp, nation, region
     * WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
     * AND p_size = [SIZE] AND p_type like '%[TYPE]' AND r_name = '[REGION]'
     *
     * SIZE is randomly selected within [1..50]
     * TYPE is randomly selected within the list Syllable 3 defined for Types in Clause 4.2.2.13
     * REGION is randomly selected within the list of values defined for R_NAME in 4.2.3
     */
    fun q2(): Query {
        val queryBuilder = QueryBuilder()
        queryBuilder.from(TpcHConstants.part)
                .from(TpcHConstants.supplier)
                .from(TpcHConstants.partsupp)
                .from(TpcHConstants.nation)
                .from(TpcHConstants.region)
                .where(BinaryPredicate.fromString("${TpcHConstants.part}.${TpcHConstants.partkey} = ${TpcHConstants.partsupp}.${TpcHConstants.partkey}"))
                .where(BinaryPredicate.fromString("${TpcHConstants.partsupp}.${TpcHConstants.suppkey} = ${TpcHConstants.supplier}.${TpcHConstants.suppkey}"))
                .where(BinaryPredicate.fromString("${TpcHConstants.supplier}.${TpcHConstants.nationkey} = ${TpcHConstants.nation}.${TpcHConstants.nationkey}"))
                .where(BinaryPredicate.fromString("${TpcHConstants.nation}.${TpcHConstants.regionkey} = ${TpcHConstants.region}.${TpcHConstants.regionkey}"))
                .to("result")
        return queryBuilder.build()
    }

    /**
     * SELECT l_orderkey, o_orderdate, o_shippriority
     * FROM customer, orders, lineitem
     * WHERE c_custkey = o_custkey and l_orderkey = o_orderkey
     * AND c_mktsegment = '[SEGMENT]' AND o_orderdate < date '[DATE]' AND l_shipdate > date '[DATE]'
     *
     * SEGMENT is randomly selected within the list of values defined for Segments in Clause 4.2.2.13
     * DATE is a randomly selected day within [1995-03-01 .. 1995-03-31].
     */
    fun q3(): Query {
        val queryBuilder = QueryBuilder()
        queryBuilder.from("customer")
                .from("orders")
                .from("lineitem")
                .where(BinaryPredicate.fromString("customer.custkey = orders.custkey"))
                .where(BinaryPredicate.fromString("orders.orderkey = lineitem.orderkey"))
                .to("result")
        return queryBuilder.build()
    }

    /**
     * SELECT n_name
     * FROM customer, orders, lineitem, supplier, nation, region
     * WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
     * AND r_name = '[REGION]' AND o_orderdate >= date '[DATE]' AND o_orderdate < date '[DATE]' + interval '1' year
     *
     * REGION is randomly selected within the list of values defined for R_NAME in Clause 4.2.3;
     * DATE is the first of January of a randomly selected year within [1993 .. 1997].
     */
    fun q5(): Query {
        val queryBuilder = QueryBuilder()
        queryBuilder.from("customer")
                .from("orders")
                .from("lineitem")
                .from("supplier")
                .from("nation")
                .from("region")
                .where(BinaryPredicate.fromString("customer.custkey = orders.custkey"))
                .where(BinaryPredicate.fromString("lineitem.orderkey = orders.orderkey"))
                .where(BinaryPredicate.fromString("lineitem.suppkey = supplier.suppkey"))
                .where(BinaryPredicate.fromString("customer.nationkey = supplier.nationkey"))
                .where(BinaryPredicate.fromString("supplier.nationkey = nation.nationkey"))
                .where(BinaryPredicate.fromString("nation.regionkey = region.regionkey"))
                .to("result")
        return queryBuilder.build()
    }

    /**
     * SELECT ps_partkey
     * FROM partsupp, supplier, nation
     * WHERE ps_suppkey = s_suppkey and s_nationkey = n_nationkey
     * AND n_name = '[NATION]'
     *
     * NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3;
     */
    fun q11(): Query {
        val queryBuilder = QueryBuilder()
        queryBuilder.from("partsupp")
                .from("supplier")
                .from("nation")
                .where(BinaryPredicate.fromString("partsupp.suppkey = supplier.suppkey"))
                .where(BinaryPredicate.fromString("supplier.nationkey = nation.nationkey"))
                .to("result")
        return queryBuilder.build()
    }
}

object TpcHOnlyJoins {

    /**
     * SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
     * FROM part, supplier, partsupp, nation, region
     * WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
     */
    fun q2(): Query {
        val queryBuilder = QueryBuilder()
        queryBuilder.from(TpcHConstants.part)
                .from(TpcHConstants.supplier)
                .from(TpcHConstants.partsupp)
                .from(TpcHConstants.nation)
                .from(TpcHConstants.region)
                .where(BinaryPredicate.fromString("part.partkey = partsupp.partkey"))
                .where(BinaryPredicate.fromString("partsupp.suppkey = supplier.suppkey"))
                .where(BinaryPredicate.fromString("supplier.nationkey = nation.nationkey"))
                .where(BinaryPredicate.fromString("nation.regionkey = region.regionkey"))
                .to("result")
        return queryBuilder.build()
    }

    /**
     * SELECT l_orderkey, o_orderdate, o_shippriority
     * FROM customer, orders, lineitem
     * WHERE c_custkey = o_custkey and l_orderkey = o_orderkey
     */
    fun q3(): Query {
        val queryBuilder = QueryBuilder()
        queryBuilder.from("customer")
                .from("orders")
                .from("lineitem")
                .where(BinaryPredicate.fromString("customer.custkey = orders.custkey"))
                .where(BinaryPredicate.fromString("orders.orderkey = lineitem.orderkey"))
                .to("result")
        return queryBuilder.build()
    }

    /**
     * SELECT n_name
     * FROM customer, orders, lineitem, supplier, nation, region
     * WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
     */
    fun q5(): Query {
        val queryBuilder = QueryBuilder()
        queryBuilder.from("customer")
                .from("orders")
                .from("lineitem")
                .from("supplier")
                .from("nation")
                .from("region")
                .where(BinaryPredicate.fromString("customer.custkey = orders.custkey"))
                .where(BinaryPredicate.fromString("lineitem.orderkey = orders.orderkey"))
                .where(BinaryPredicate.fromString("lineitem.suppkey = supplier.suppkey"))
                .where(BinaryPredicate.fromString("customer.nationkey = supplier.nationkey"))
                .where(BinaryPredicate.fromString("supplier.nationkey = nation.nationkey"))
                .where(BinaryPredicate.fromString("customer.nationkey = nation.nationkey"))
                .where(BinaryPredicate.fromString("nation.regionkey = region.regionkey"))
                .to("result")
        return queryBuilder.build()
    }

    /**
     * SELECT ps_partkey
     * FROM partsupp, supplier, nation
     * WHERE ps_suppkey = s_suppkey and s_nationkey = n_nationkey
     */
    fun q11(): Query {
        val queryBuilder = QueryBuilder()
        queryBuilder.from("partsupp")
                .from("supplier")
                .from("nation")
                .where(BinaryPredicate.fromString("partsupp.suppkey = supplier.suppkey"))
                .where(BinaryPredicate.fromString("supplier.nationkey = nation.nationkey"))
                .to("result")
        return queryBuilder.build()
    }
}

object TpchContinuous {
    /**
     * SELECT l_returnflag, l_linestatus,
     *        sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
     *        avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc,
     *        count(*) as count_order
     * FROM lineitem
     * WHERE l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
     * GROUP BY l_returnflag, l_linestatus
     *
     * DELTA is randomly selected within [60..120].
     */
    fun q1(): Query {
        val queryBuilder = QueryBuilder()
        queryBuilder.from("lineitem")
            .where("lineitem.shipdate <= date '1998-12-01'") // TODO add interval subtraction
            .groupBy("returnflag", "linestatus")
            .select("returnflag")
            .select("linestatus")
            .select("count(*)")
        return queryBuilder.build()
    }
}
