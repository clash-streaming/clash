package de.unikl.dbis.clash.physical

import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.Relation
import java.io.Serializable

/**
 * Common properties of all rules.
 */
interface Rule : Serializable

/**
 * An InRule is a rule which provides an input stream label.
 */
interface InRule : Rule {

    val incomingEdgeLabel: EdgeLabel

    fun replaceIncomingEdgeLabel(newLabel: EdgeLabel): InRule
}

/**
 * A OldJoinRule is a rule which provides an output stream label.
 */
@Deprecated("Use JoinRule with BinaryPredicateEvaluation instead!")
internal interface OldJoinRule : Rule {

    val predicates: Set<BinaryPredicate>
}

/**
 * A OldJoinRule is a rule which provides an output stream label.
 */
internal interface JoinRule : Rule {

    val predicates: Set<BinaryPredicateEvaluation>
}

/**
 * An OutRule is a rule which provides an output stream label.
 */
interface OutRule : Rule {

    val outgoingEdgeLabel: EdgeLabel

    fun replaceOutgoingEdgeLabel(newLabel: EdgeLabel): OutRule
}

class ReportInRule(override val incomingEdgeLabel: EdgeLabel) : InRule {

    override fun replaceIncomingEdgeLabel(newLabel: EdgeLabel): InRule {
        return ReportInRule(newLabel)
    }

    override fun toString(): String {
        return "ReportInRule: send to " + this.incomingEdgeLabel
    }
}

/**
 * This rule indicates where reporting messages should be sent.
 */
class ReportOutRule(override val outgoingEdgeLabel: EdgeLabel) : OutRule {

    override fun replaceOutgoingEdgeLabel(newLabel: EdgeLabel): OutRule {
        return ReportOutRule(newLabel)
    }

    override fun toString(): String {
        return "ReportOutRule: send to " + this.outgoingEdgeLabel
    }
}

class ControlInRule(override val incomingEdgeLabel: EdgeLabel) : InRule {
    override fun replaceIncomingEdgeLabel(newLabel: EdgeLabel): InRule {
        return ControlInRule(newLabel)
    }
}

class ControlOutRule(override val outgoingEdgeLabel: EdgeLabel) : OutRule {
    override fun replaceOutgoingEdgeLabel(newLabel: EdgeLabel): OutRule {
        return ControlOutRule(newLabel)
    }
}

class TickInRule(override val incomingEdgeLabel: EdgeLabel) : InRule {
    override fun replaceIncomingEdgeLabel(newLabel: EdgeLabel): InRule {
        return TickInRule(newLabel)
    }
}

class TickOutRule(override val outgoingEdgeLabel: EdgeLabel) : OutRule {
    override fun replaceOutgoingEdgeLabel(newLabel: EdgeLabel): OutRule {
        return TickOutRule(newLabel)
    }
}

/**
 * This rule indicates that if a tuple arrives at Storm stream incomingStreamName, it is a part of
 * the streamed relation relation.
 */
class RelationReceiveRule(
    val relation: Relation,
    override val incomingEdgeLabel: EdgeLabel
) : InRule {

    override fun toString(): String {
        return "RelationReceiveRule: receive tuples for " + this.relation
    }

    override fun replaceIncomingEdgeLabel(newLabel: EdgeLabel): InRule {
        return RelationReceiveRule(this.relation, newLabel)
    }
}

/**
 * This rule indicates that if a tuple of streamed Relation relation gets known
 * (e.g. by a join), it has to be output via the Storm stream outgoingStreamName.
 */
class RelationSendRule
/**
 * @param relation Name of the streamed relation the generated result belongs to (e.g. "ab")
 * @param outgoingEdgeLabel Name of the stream the tuple is sent over (e.g. "s_167")
 */(
     val relation: Relation,
     override val outgoingEdgeLabel: EdgeLabel
 ) : OutRule {

    override fun toString(): String {
        return ("RelationSendRule: send result for " + this.relation +
                " to " + this.outgoingEdgeLabel)
    }

    override fun replaceOutgoingEdgeLabel(newLabel: EdgeLabel): OutRule {
        return RelationSendRule(this.relation, newLabel)
    }
}

class IntermediateJoinRule
/**
 * Create a new OldIntermediateJoinRule.
 *
 * @param incomingEdgeLabel label of the incoming stream
 * @param outgoingEdgeLabel label of the outgoing stream
 * @param predicates predicates that have to hold
 */(
     override val incomingEdgeLabel: EdgeLabel,
     override val outgoingEdgeLabel: EdgeLabel,
     override val predicates: Set<BinaryPredicateEvaluation>
 ) : InRule, JoinRule, OutRule {

    override fun toString(): String {
        return ("OldJoinRule: join from " + incomingEdgeLabel + " and send to " + outgoingEdgeLabel +
                " if " + predicates.size + " predicates match:" +
                predicates.joinToString { " ∧ " })
    }

    override fun replaceIncomingEdgeLabel(newLabel: EdgeLabel): InRule {
        return IntermediateJoinRule(newLabel, this.outgoingEdgeLabel, this.predicates)
    }

    override fun replaceOutgoingEdgeLabel(newLabel: EdgeLabel): OutRule {
        return IntermediateJoinRule(this.incomingEdgeLabel, newLabel, this.predicates)
    }
}

/**
 * Instantiate a new JoinResultRule.
 *
 * @param incomingEdgeLabel Name of the stream where data comes from
 * @param predicates Predicates that have to be satisfied
 * @param relation The relation that is produced with all successful joins
 */
class JoinResultRule(
    override val incomingEdgeLabel: EdgeLabel,
    override val predicates: Set<BinaryPredicateEvaluation>,
    val relation: Relation
) : InRule, JoinRule {
    override fun toString(): String {
        return "JoinResultRule: join from $incomingEdgeLabel and use as result if " + predicates
                .size + " predicates match: " + predicates.map { it.predicate }
    }

    override fun replaceIncomingEdgeLabel(newLabel: EdgeLabel): InRule {
        return JoinResultRule(newLabel, this.predicates, this.relation)
    }
}

class StoreAndJoinRule
/**
 * Instantiate a new store and join rule.
 * @param relationName Name of the relation that should be stored
 * @param incomingEdgeLabel Name of the stream whose tuples should be stored
 * @param predicates Predicates that have to be satisfied
 */(
     val relationName: String,
     override val incomingEdgeLabel: EdgeLabel,
     override val predicates: Set<BinaryPredicate>
 ) : InRule, OldJoinRule {

    override fun replaceIncomingEdgeLabel(newLabel: EdgeLabel): InRule {
        return StoreAndJoinRule(this.relationName, newLabel, this.predicates)
    }

    override fun toString(): String {
        return ("StoreAndJoinRule: store document of relation " + this.relationName +
                " coming on stream " + this.incomingEdgeLabel + " and join with others if " +
                this.predicates.size + " predicates match")
    }
}

/**
 * A BinaryPredicate has a left and a right AttributeAccess.
 * For example, in the predicate "x.age < y.age",
 * the left AttributeAccess is "x.age" and the right is "y.age".
 *
 * The same predicate is used for evaluation in the x-store and in the y-store.
 * However, it is crucial to know, if an arriving probe tuple should serve as left or right input.
 *
 * For this, the implementations of BinaryPredicateEvaluation can be used.
 * In the above example, at the x-store a JoinResultRule would be registered with
 * BinaryPredicateEvaluationLeftStored, such that the left AttributeAccess would
 * go to the stored tuples and the right AttributeAccess to the arriving tuple.
 * Vice-versa, the BinaryPredicateEvaluationRightStored is used at the y-store.
 */
interface BinaryPredicateEvaluation : Serializable {
    val predicate: BinaryPredicate
}
data class GenericBinaryPredicateEvaluation(override val predicate: BinaryPredicate) : BinaryPredicateEvaluation
data class BinaryPredicateEvaluationLeftStored(override val predicate: BinaryPredicate) : BinaryPredicateEvaluation
data class BinaryPredicateEvaluationRightStored(override val predicate: BinaryPredicate) : BinaryPredicateEvaluation
