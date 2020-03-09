package de.unikl.dbis.clash.flexstorm.control

/**
 * This stream name is used when the control spout sends a command
 * to all other bolts.
 */
const val CONTROL_SPOUT_TO_ALL_STREAM_NAME = "_ctrl2all"

/**
 * This stream name is used when the control spout sends a command
 * to all flex bolt instances.
 */
const val CONTROL_SPOUT_TO_FLEX_BOLT_STREAM_NAME = "_ctrl2flex"

/**
 * This stream name is used when the control spout sends a command
 * to the control bolt
 */
const val CONTROL_SPOUT_TO_CONTROL_BOLT_STREAM_NAME = "_ctrl2ctrl"

/**
 * This stream name is used when a component has a message already
 * prepared and the control bolt should send it to the manager.
 */
const val FORWARD_TO_CONTROL_BOLT_STREAM_NAME = "_fwd2ctrl"

/**
 * This stream name is used for sending tick signals to the control bolt.
 */
const val TICK_SPOUT_TO_CONTROL_BOLT_STREAM_NAME = "_tick2ctrl"

/**
 * This stream name is used for broadcasting control messages to all flex bolts.
 */
const val CONTROL_BOLT_TO_ALL_FLEX_BOLTS_STREAM_NAME = "_ctrlbolt2flexAll"

/**
 * This stream name is used for sending control messages to individual flex bolts.
 */
const val CONTROL_BOLT_TO_FLEX_BOLT_STREAM_NAME = "_ctrlbolt2flex"

/**
 * This stream name is used for sending messages to from flex bolts to the control bolt.
 */
const val FLEX_BOLT_TO_CONTROL_BOLT_STREAM_NAME = "_flex2ctrl"
