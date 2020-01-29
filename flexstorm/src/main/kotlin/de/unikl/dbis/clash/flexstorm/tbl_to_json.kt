package de.unikl.dbis.clash.flexstorm

import com.google.gson.JsonObject

fun tblToJson(raw: String, tablename: String): JsonObject {
    val result = JsonObject()
    return when(tablename) {
        "customer" -> customerTblToJson(raw)
        "order", "orders" -> orderTblToJson(raw)
        "lineitem" -> lineitemTblToJson(raw)
        else -> TODO("don't know how to convert table $tablename")
    }
}

/**
 * 1|Customer#000000001|j5BkijBM1PkCy0O1m|15|25-989-989-0989|711.56|BUILDING|AN0PL zMkz5kz6N64kM wnAxz6MMgx5B6Nl1ljnM4i QmA4ixB nMgB11jQRi61z70 76NlmCMNPg1yhQjR7myP0|
 */
fun customerTblToJson(raw: String): JsonObject {
    val entries = raw.split('|')
    val result = JsonObject()
    result.addProperty("custkey", entries[0])
    result.addProperty("name", entries[1])
    result.addProperty("address", entries[2])
    result.addProperty("nationkey", entries[3])
    result.addProperty("phone", entries[4])
    result.addProperty("acctbal", entries[5])
    result.addProperty("mktsegment", entries[6])
    result.addProperty("comment", entries[7])

    return result
}

/**
 * 1|78002|O|124367.58|1996-01-02|5-LOW|Clerk#000000951|0|jSmgxOi157kxm gCwQzgNOmiO0MkhCR4N BLj6OQBkl34kj2yly5RzlOzx0A1Chi2j|
 */
fun orderTblToJson(raw: String): JsonObject {
    val entries = raw.split('|')
    val result = JsonObject()
    result.addProperty("orderkey", entries[0])
    result.addProperty("custkey", entries[1])
    result.addProperty("orderstatus", entries[2])
    result.addProperty("totalprice", entries[3])
    result.addProperty("orderdate", entries[4])
    result.addProperty("orderpriority", entries[5])
    result.addProperty("clerk", entries[6])
    result.addProperty("shippriority", entries[7])
    result.addProperty("comment", entries[8])

    return result
}

/**
 * 1|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|B3hnOy6xLnMz2jk637jARMR434|
 */
fun lineitemTblToJson(raw: String): JsonObject {
    val entries = raw.split('|')
    val result = JsonObject()
    result.addProperty("orderkey", entries[0])
    result.addProperty("partkey", entries[1])
    result.addProperty("suppkey", entries[2])
    result.addProperty("linenumber", entries[3])
    result.addProperty("quantity", entries[4])
    result.addProperty("extendedprice", entries[5])
    result.addProperty("discount", entries[6])
    result.addProperty("tax", entries[7])
    result.addProperty("returnflag", entries[8])
    result.addProperty("linestatus", entries[9])
    result.addProperty("shipdate", entries[10])
    result.addProperty("commitdate", entries[11])
    result.addProperty("receiptdate", entries[12])
    result.addProperty("shipinstruct", entries[13])
    result.addProperty("shipmode", entries[14])
    result.addProperty("comment", entries[15])

    return result
}
