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
