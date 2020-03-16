package de.unikl.dbis.clash.storm

import de.unikl.dbis.clash.physical.AggregateRule
import de.unikl.dbis.clash.physical.BinaryPredicateEvaluation
import de.unikl.dbis.clash.physical.ControlInRule
import de.unikl.dbis.clash.physical.ControlOutRule
import de.unikl.dbis.clash.physical.IntermediateJoinRule
import de.unikl.dbis.clash.physical.JoinResultRule
import de.unikl.dbis.clash.physical.RelationReceiveRule
import de.unikl.dbis.clash.physical.RelationSendRule
import de.unikl.dbis.clash.physical.Rule
import de.unikl.dbis.clash.physical.SelectProjectRule
import de.unikl.dbis.clash.physical.StoreAndJoinRule
import de.unikl.dbis.clash.physical.TickInRule
import de.unikl.dbis.clash.physical.TickOutRule
import de.unikl.dbis.clash.physical.UnaryPredicateEvaluation
import de.unikl.dbis.clash.query.Aggregation
import de.unikl.dbis.clash.query.AttributeAccess
import de.unikl.dbis.clash.query.BinaryPredicate
import de.unikl.dbis.clash.query.ProjectionList
import de.unikl.dbis.clash.query.Relation
import java.io.Serializable

/**
 * Common properties of all rules.
 */
interface StormRule : Serializable {

    /**
     * @return The MessageVariant this message is associated with
     */
    val messageVariant: MessageVariant

    companion object {

        fun stormRuleFromRule(rule: Rule): StormRule {
            return when (rule) {
                is IntermediateJoinRule -> StormIntermediateJoinRule(rule)
                is JoinResultRule -> StormJoinResultRule(rule)
                is StoreAndJoinRule -> StormStoreAndJoinRule(rule)
                is RelationReceiveRule -> StormRelationReceiveRule(rule)
                is RelationSendRule -> StormRelationSendRule(rule)
                is ControlInRule -> StormControlInRule(rule)
                is ControlOutRule -> StormControlOutRule(rule)
                is TickInRule -> StormTickInRule(rule)
                is TickOutRule -> StormTickOutRule(rule)
                is SelectProjectRule -> StormSelectProjectRule(rule)
                is AggregateRule -> StormAggregateRule(rule)
                else -> throw RuntimeException("Cannot convert rule $rule to StormRule")
            }
        }
    }
}

/**
 * An InRule is a rule which provides an input stream label.
 */
interface StormInRule : StormRule {

    val incomingEdgeLabel: StormEdgeLabel
}

/**
 * An OutRule is a rule which provides an output stream label.
 */
interface StormOutRule : StormRule {

    val outgoingEdgeLabel: StormEdgeLabel
}

/**
 * A OldJoinRule is a rule which provides an output stream label.
 */
internal interface StormJoinRule : StormRule {

    val predicates: Set<BinaryPredicateEvaluation>

    override val messageVariant: MessageVariant
        get() = MessageVariant.DataPath
}

/**
 * This rule indicates that if a tuple arrives at Storm stream incomingStreamName, it is a part of
 * the streamed relation relation.
 */
class StormRelationReceiveRule : StormInRule, StormDataPathRule {

    val relation: Relation
    override val incomingEdgeLabel: StormEdgeLabel

    override val messageVariant: MessageVariant
        get() = MessageVariant.DataPath

    /**
     * Instantiate a new stream receive rule.
     *
     * @param rule the physical graph's rule that is modelled
     */
    constructor(rule: RelationReceiveRule) {
        this.relation = rule.relation
        this.incomingEdgeLabel = StormEdgeLabel(rule.incomingEdgeLabel)
    }

    constructor(
        relation: Relation,
        incomingEdgeLabel: StormEdgeLabel
    ) {
        this.relation = relation
        this.incomingEdgeLabel = incomingEdgeLabel
    }
}

/**
 * This rule indicates that if a tuple of streamed Relation relation gets known (e.g. by a
 * join), it has to be output via the Storm stream outgoingStreamName.
 */
class StormRelationSendRule : StormOutRule, StormDataPathRule {

    val relation: Relation
    override val outgoingEdgeLabel: StormEdgeLabel

    override val messageVariant: MessageVariant
        get() = MessageVariant.DataPath

    /**
     * Instantiate a new stream send rule.
     *
     * @param rule the physical graph's rule that is modelled
     */
    constructor(rule: RelationSendRule) {
        this.relation = rule.relation
        this.outgoingEdgeLabel = StormEdgeLabel(rule.outgoingEdgeLabel)
    }

    /**
     * Instantiate a new stream send rule.
     *
     * @param relation the label of the sent relation
     * @param outgoingEdgeLabel the label the relation should be sent to
     */
    constructor(
        relation: Relation,
        outgoingEdgeLabel: StormEdgeLabel
    ) {
        this.relation = relation
        this.outgoingEdgeLabel = outgoingEdgeLabel
    }

    override fun toString(): String {
        return ("StreamSendRule: send result for " + this.relation +
                " to " + this.outgoingEdgeLabel)
    }
}

class StormStoreAndJoinRule : StormInRule, StormDataPathRule {

    val relationName: String
    val predicates: Set<BinaryPredicateEvaluation>
    override val incomingEdgeLabel: StormEdgeLabel

    override val messageVariant: MessageVariant
        get() = MessageVariant.DataPath

    /**
     * Instantiate a new store and join rule.
     *
     * @param rule the physical graph's rule that is modelled
     */
    constructor(rule: StoreAndJoinRule) {
        this.relationName = rule.relationName
        this.predicates = rule.predicates
        this.incomingEdgeLabel = StormEdgeLabel(rule.incomingEdgeLabel)
    }

    override fun toString(): String {
        return ("StoreAndJoinRule: store document of relation " + this.relationName +
                " coming on stream " + this.incomingEdgeLabel + " and join with others if " +
                this.predicates.size + " predicates match")
    }
}

class StormJoinResultRule : StormInRule, StormJoinRule, StormDataPathRule {

    override val predicates: Set<BinaryPredicateEvaluation>

    val relation: Relation
    override val incomingEdgeLabel: StormEdgeLabel

    constructor(rule: JoinResultRule) {
        this.relation = rule.relation
        this.predicates = rule.predicates
        this.incomingEdgeLabel = StormEdgeLabel(rule.incomingEdgeLabel)
    }

    override fun toString(): String {
        return "JoinResultRule: join from $incomingEdgeLabel and use as result if " + predicates
                .size + " predicates match: " + predicates
    }
}

class StormSelectProjectRule : StormInRule, StormOutRule, StormDataPathRule {
    val predicates: Set<UnaryPredicateEvaluation>
    val projection: ProjectionList

    constructor(rule: SelectProjectRule) {
        predicates = rule.predicates
        projection = rule.projection
        incomingEdgeLabel = StormEdgeLabel(rule.incomingEdgeLabel)
        outgoingEdgeLabel = StormEdgeLabel(rule.outgoingEdgeLabel)
    }

    override val incomingEdgeLabel: StormEdgeLabel
    override val outgoingEdgeLabel: StormEdgeLabel

    override val messageVariant: MessageVariant
        get() = MessageVariant.DataPath
}

class StormAggregateRule : StormInRule, StormOutRule {
    val aggregates: List<Aggregation>

    constructor(rule: AggregateRule) {
        aggregates = rule.aggregates
        incomingEdgeLabel = StormEdgeLabel(rule.incomingEdgeLabel)
        outgoingEdgeLabel = StormEdgeLabel(rule.outgoingEdgeLabel)
    }

    override val incomingEdgeLabel: StormEdgeLabel
    override val outgoingEdgeLabel: StormEdgeLabel
    override val messageVariant: MessageVariant
        get() = MessageVariant.DataPath
}

/**
 * An IntermediateJoinRule is a rule of type "if a tuple arrives at the stream inputStreamName, then
 * join it using predicate and send emerging results to stream outputStreamName".
 */
class StormIntermediateJoinRule : StormInRule, StormJoinRule, StormOutRule, StormDataPathRule {

    override val predicates: Set<BinaryPredicateEvaluation>
    override val incomingEdgeLabel: StormEdgeLabel
    override val outgoingEdgeLabel: StormEdgeLabel

    /**
     * Create a new OldIntermediateJoinRule.
     *
     * @param rule the physical graph's rule that is modelled
     */
    constructor(rule: IntermediateJoinRule) {
        this.predicates = rule.predicates
        this.incomingEdgeLabel = StormEdgeLabel(rule.incomingEdgeLabel)
        this.outgoingEdgeLabel = StormEdgeLabel(rule.outgoingEdgeLabel)
    }

    override fun toString(): String {
        return ("OldJoinRule: join from " + incomingEdgeLabel + " and send to " + outgoingEdgeLabel +
                " if " + predicates.size + " predicates match:" +
                predicates.joinToString(" ∧ "))
    }
}

/**
 * MarkerInterface for all StormRules that influence behaviour on the ControlPath
 */
interface StormControlRule : StormRule

class StormControlInRule : StormInRule, StormControlRule {

    override val incomingEdgeLabel: StormEdgeLabel

    override val messageVariant: MessageVariant
        get() = MessageVariant.Control

    /**
     * Instantiate a new stream receive rule.
     *
     * @param rule the physical graph's rule that is modelled
     */
    constructor(rule: ControlInRule) {
        this.incomingEdgeLabel = StormEdgeLabel(rule.incomingEdgeLabel)
    }

    constructor(incomingEdgeLabel: StormEdgeLabel) {
        this.incomingEdgeLabel = incomingEdgeLabel
    }

    override fun toString(): String {
        return "ControlRule: receive from " + this.incomingEdgeLabel
    }
}

class StormControlOutRule : StormOutRule, StormControlRule {

    override val outgoingEdgeLabel: StormEdgeLabel

    override val messageVariant: MessageVariant
        get() = MessageVariant.Control

    constructor(rule: ControlOutRule) {
        this.outgoingEdgeLabel = StormEdgeLabel(rule.outgoingEdgeLabel)
    }

    constructor(outgoingEdgeLabel: StormEdgeLabel) {
        this.outgoingEdgeLabel = outgoingEdgeLabel
    }

    override fun toString(): String {
        return "ControlRule: send to " + this.outgoingEdgeLabel
    }
}

/**
 * MarkerInterface for all StormRules that influence behaviour on the ControlPath
 */
interface StormTickRule : StormRule

class StormTickInRule : StormInRule, StormTickRule {

    override val incomingEdgeLabel: StormEdgeLabel

    override val messageVariant: MessageVariant
        get() = MessageVariant.Tick

    /**
     * Instantiate a new stream receive rule.
     *
     * @param rule the physical graph's rule that is modelled
     */
    constructor(rule: TickInRule) {
        this.incomingEdgeLabel = StormEdgeLabel(rule.incomingEdgeLabel)
    }

    constructor(incomingEdgeLabel: StormEdgeLabel) {
        this.incomingEdgeLabel = incomingEdgeLabel
    }

    override fun toString(): String {
        return "TickRule: receive from $incomingEdgeLabel"
    }
}

class StormTickOutRule : StormOutRule, StormTickRule {

    override val outgoingEdgeLabel: StormEdgeLabel

    override val messageVariant: MessageVariant
        get() = MessageVariant.Tick

    constructor(rule: TickOutRule) {
        this.outgoingEdgeLabel = StormEdgeLabel(rule.outgoingEdgeLabel)
    }

    constructor(outgoingEdgeLabel: StormEdgeLabel) {
        this.outgoingEdgeLabel = outgoingEdgeLabel
    }

    override fun toString(): String {
        return "TickRule: send to $outgoingEdgeLabel"
    }
}

/**
 * MarkerInterface for all StormRules that influence behaviour on the DataPath
 */
interface StormDataPathRule : StormRule
