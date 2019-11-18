package de.unikl.dbis.clash.query


data class ConstantEquality<T>(
        override val attributeAccess: AttributeAccess,
        val constant: T) : UnaryAttributePredicate {
    override fun evaluate(tuple: Tuple): Boolean {
        return tuple[attributeAccess] == constant
    }

    override fun toString(): String {
        return "$attributeAccess = $constant"
    }
}

data class UnaryLike(
        override val attributeAccess: AttributeAccess,
        val likeExpr: String
) : UnaryAttributePredicate {
    override fun evaluate(tuple: Tuple): Boolean {
        val subject = tuple[attributeAccess]!!

        // LIKE '%foo%'
        if(likeExpr.startsWith("$") && likeExpr.endsWith("$")) {
            val term = likeExpr.subSequence(1, likeExpr.length - 1)
            return subject.contains(term)
        }

        // LIKE '%foo'
        if(likeExpr.startsWith("$") && !likeExpr.endsWith("$")) {
            val term = likeExpr.subSequence(1, likeExpr.length).toString()
            return subject.indexOf(term) == subject.length - term.length
        }

        // LIKE 'foo%'
        if(!likeExpr.startsWith("$") && likeExpr.endsWith("$")) {
            val term = likeExpr.subSequence(0, likeExpr.length - 1).toString()
            return subject.indexOf(term) == 0
        }

        throw RuntimeException("Do not understand LIKE expression $likeExpr")
    }

    override fun toString(): String {
        return "$attributeAccess LIKE '$likeExpr'"
    }
}

data class AttributeLessThanConstant<T>(
        override val attributeAccess: AttributeAccess,
        val constant: T): UnaryAttributePredicate {
    override fun evaluate(tuple: Tuple): Boolean {
        return when(constant) {
            is Long -> tuple[attributeAccess]!!.toLong() < constant
            is Double -> tuple[attributeAccess]!!.toDouble() < constant
            else -> tuple[attributeAccess]!! < constant.toString()
        }
    }

    override fun toString(): String {
        return "$attributeAccess < $constant"
    }
}

data class AttributeGreaterThanConstant<T>(
        override val attributeAccess: AttributeAccess,
        val constant: T): UnaryAttributePredicate {
    override fun evaluate(tuple: Tuple): Boolean {
        return when(constant) {
            is Long -> tuple[attributeAccess]!!.toLong() > constant
            is Double -> tuple[attributeAccess]!!.toDouble() > constant
            else -> tuple[attributeAccess]!! > constant.toString()
        }
    }

    override fun toString(): String {
        return "$attributeAccess > $constant"
    }
}


data class AttributeLessThanOrEqualConstant<T>(
        override val attributeAccess: AttributeAccess,
        val constant: T): UnaryAttributePredicate {
    override fun evaluate(tuple: Tuple): Boolean {
        return when(constant) {
            is Long -> tuple[attributeAccess]!!.toLong() <= constant
            is Double -> tuple[attributeAccess]!!.toDouble() <= constant
            else -> tuple[attributeAccess]!! <= constant.toString()
        }
    }

    override fun toString(): String {
        return "$attributeAccess <= $constant"
    }
}

data class AttributeGreaterThanOrEqualConstant<T>(
        override val attributeAccess: AttributeAccess,
        val constant: T): UnaryAttributePredicate {
    override fun evaluate(tuple: Tuple): Boolean {
        return when(constant) {
            is Long -> tuple[attributeAccess]!!.toLong() >= constant
            is Double -> tuple[attributeAccess]!!.toDouble() >= constant
            else -> tuple[attributeAccess]!! >= constant.toString()
        }
    }

    override fun toString(): String {
        return "$attributeAccess >= $constant"
    }
}