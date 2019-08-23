package de.unikl.dbis.clash.storm

import de.unikl.dbis.clash.physical.Rule
import de.unikl.dbis.clash.query.Relation
import de.unikl.dbis.clash.storm.StormRule.Companion.stormRuleFromRule
import java.io.Serializable


/**
 * The SI (Single-Input)-Rule-Set assumes, that for every stream there is a single rule defining
 * what happens with the input tuple.
 */
class SiRuleSet : Serializable {
    private val inRules = mutableMapOf<String, StormInRule>()
    private val outRules = mutableSetOf<StormOutRule>()

    fun add(rule: Rule) {
        this.add(stormRuleFromRule(rule))
    }

    fun add(stormRule: StormRule) {
        var found = false
        if (stormRule is StormInRule) {
            inRules[stormRule.incomingEdgeLabel.streamName] = stormRule
            found = true
        }
        if (stormRule is StormOutRule) {
            outRules.add(stormRule)
            found = true
        }
        if (!found) {
            throw RuntimeException("Trying to process a rule that is neither StormInRule nor"
                    + "StormOutRule: " + stormRule)
        }
    }

    /**
     * @return a stream of all registered OutRules
     */
    fun outRules(): Collection<StormOutRule> {
        return this.outRules.toList()
    }

    /**
     * @return a stream of all registered InRules
     */
    fun inRules(): Collection<StormInRule> {
        return this.inRules.values.toList()
    }

    /**
     * @return the single InRule associated with the incoming stream streamName
     */
    fun inRuleFor(streamName: String): StormInRule {
        return this.inRules[streamName] ?: TODO("This should not happen!")
    }


    /**
     * @return a stream of all registered ReportInRules
     */
    fun controlInRules(): Collection<StormControlInRule> {
        return this.inRules()
                .filter { StormControlInRule::class.java.isInstance(it) }
                .map { StormControlInRule::class.java.cast(it) }
    }

    /**
     * @return a stream of all registered ReportOutRules
     */
    fun controlOutRules(): Collection<StormControlOutRule> {
        return this.outRules()
                .filter { StormControlOutRule::class.java.isInstance(it) }
                .map { StormControlOutRule::class.java.cast(it) }
    }

    /**
     * @param relation the label of the relation
     * @return a stream of all registered StormRelationSendRules for this relation
     */
    fun relationSendRulesFor(relation: Relation): Collection<StormRelationSendRule> {
        return this.outRules()
                .filter { StormRelationSendRule::class.java.isInstance(it) }
                .map { StormRelationSendRule::class.java.cast(it) }
                .filter { x -> x.relation == relation }
    }
}