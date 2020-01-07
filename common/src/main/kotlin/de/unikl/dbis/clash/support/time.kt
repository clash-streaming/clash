package de.unikl.dbis.clash.support

const val MILLIS_PER_SECOND = 1000
const val SECONDS_PER_MINUTE = 60

fun minutesToMillis(minutes: Long): Long {
    return minutes * SECONDS_PER_MINUTE * MILLIS_PER_SECOND
}
