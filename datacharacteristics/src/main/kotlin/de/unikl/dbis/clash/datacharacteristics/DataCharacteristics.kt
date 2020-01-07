package de.unikl.dbis.clash.datacharacteristics

import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.Relation
import de.unikl.dbis.clash.query.RelationAlias

interface DataCharacteristics {

    /**
     * @param relation the relation that should be produced
     * @return The output rate of that relation
     */
    fun getRate(relation: Relation): Double

    /**
     * @param relationAlias name of the stream
     * @return The rate of the stream identified as streamName
     */
    fun getRate(relationAlias: RelationAlias): Double

    /**
     * @param relationAlias name of the stream
     * @param rate rate of that stream in tuples per second
     */
    fun setRate(relationAlias: RelationAlias, rate: Double)

    /**
     * @param r1 name of the first stream
     * @param r2 name of the second stream
     * @return the selectivity of the join between those two source streams
     */
    fun getSelectivity(r1: RelationAlias, r2: RelationAlias): Double

    /**
     * @param predicate the join predicate
     * @return the selectivity of the join over this predicate
     */
    fun getSelectivity(predicate: BinaryPredicate): Double

    /**
     * @param r1 name of the first stream
     * @param r2 name of the second stream
     * @param selectivity selectivity of the join between those two source streams
     */
    fun setSelectivity(r1: RelationAlias, r2: RelationAlias, selectivity: Double)

    /**
     * @param predicate the join predicate
     * @param selectivity selectivity of the join over this predicate
     */
    fun setSelectivity(predicate: BinaryPredicate, selectivity: Double)
}
