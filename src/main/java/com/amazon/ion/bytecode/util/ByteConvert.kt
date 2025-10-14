package com.amazon.ion.bytecode.util

/** Converts this [Byte] to an [Int], treating this [Byte] as if it is an _unsigned_ number. */
internal fun Byte.unsignedToInt(): Int = this.toInt() and 0xFF
