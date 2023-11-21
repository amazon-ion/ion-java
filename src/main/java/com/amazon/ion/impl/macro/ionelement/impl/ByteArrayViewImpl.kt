package com.amazon.ion.impl.macro.ionelement.impl

import com.amazon.ion.impl.macro.ionelement.api.ByteArrayView

internal class ByteArrayViewImpl(private val bytes: ByteArray) : ByteArrayView {
    override fun size(): Int = bytes.size

    override fun get(index: Int): Byte = bytes[index]

    override fun copyOfBytes(): ByteArray = bytes.clone()

    override fun iterator(): Iterator<Byte> = bytes.iterator()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ByteArrayViewImpl) return false

        if (!bytes.contentEquals(other.bytes)) return false

        return true
    }

    override fun hashCode(): Int {
        return bytes.contentHashCode()
    }
}
