package de.unikl.dbis.clash.query

import org.apache.logging.log4j.LogManager

data class BinaryEquality(
        override val leftAttributeAccess: AttributeAccess,
        override val rightAttributeAccess: AttributeAccess
) : BinaryPredicate {
    override fun joinable(left: Tuple, right: Tuple): Boolean {
        return if (left[leftAttributeAccess] != null) left[leftAttributeAccess] == right[rightAttributeAccess]
        else right[leftAttributeAccess] == left[rightAttributeAccess]
    }

    override fun toString(): String {
        return "$leftAttributeAccess = $rightAttributeAccess"
    }
}

data class BinaryGreaterThan(
        override val leftAttributeAccess: AttributeAccess,
        override val rightAttributeAccess: AttributeAccess
) : BinaryPredicate {
    override fun joinable(left: Tuple, right: Tuple): Boolean {
        return left[leftAttributeAccess]!! < right[rightAttributeAccess].toString()
    }

    override fun toString(): String {
        return "$leftAttributeAccess < $rightAttributeAccess"
    }
}

data class AttributePairEquality
/**
 * Performs equality test on values of leftAttr of left and rightAttr right.
 */
(override val leftAttributeAccess: AttributeAccess,
 override val rightAttributeAccess: AttributeAccess) : BinaryPredicate {

    override fun joinable(left: Tuple,
                          right: Tuple): Boolean {
        LOG.debug("Evaluating $this")
        if (!(left[leftAttributeAccess] != null && right[rightAttributeAccess] != null)) {
            // attribute not present in both documents
            return false
        }

        if (left[leftAttributeAccess] != right[rightAttributeAccess]) {
            // value is not the same in both documents
            return false
        }
        return true
    }

    override fun toString(): String {
        return "$leftAttributeAccess = $rightAttributeAccess"
    }


    companion object {
        private val LOG = LogManager.getLogger()
    }
}
