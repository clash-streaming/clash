package de.unikl.dbis.clash.query

import java.io.Serializable

data class WindowDefinition internal constructor(val variant: Variant, val amount: Long) : Serializable {
    enum class Variant {
        None,
        TimeInS,
        Count
    }

    override fun toString(): String {
        return when (variant) {
            Variant.None -> "[∞]"
            Variant.TimeInS -> "[$amount s]"
            Variant.Count -> "[$amount t]"
        }
    }

    companion object {
        @JvmStatic
        fun seconds(seconds: Long): WindowDefinition {
            return WindowDefinition(Variant.TimeInS, seconds)
        }

        @JvmStatic
        fun minutes(minutes: Long): WindowDefinition {
            return WindowDefinition(Variant.TimeInS, minutes * 60)
        }

        @JvmStatic
        fun hours(hours: Long): WindowDefinition {
            return WindowDefinition(Variant.TimeInS, hours * 60 * 60)
        }

        @JvmStatic
        fun count(count: Long): WindowDefinition {
            return WindowDefinition(Variant.Count, count)
        }

        @JvmStatic
        fun infinite(): WindowDefinition {
            return WindowDefinition(Variant.None, 0)
        }
    }
}
