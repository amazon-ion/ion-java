// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.impl.bin.*
import com.amazon.ion.util.*
import java.io.Closeable
import java.io.Flushable

/**
 * A [_Private_FastAppendable] that buffers data to a [StringBuilder]. Only when
 * [flush] is called is the data written to the wrapped [Appendable].
 *
 * This is necessary for cases where an [IonManagedWriter_1_1] over Ion text needs to emit encoding directives that are
 * not known in advance. The [AppendableFastAppendable] class has no buffering, so system and user values would be
 * emitted in the wrong order.
 *
 * Once [IonManagedWriter_1_1] supports an auto-flush feature, then this class will have very little practical
 * difference from [AppendableFastAppendable] for the case where no system values are needed.
 *
 * TODO:
 *   - Add proper tests
 *
 * @see BufferedOutputStreamFastAppendable
 * @see AppendableFastAppendable
 */
internal class BufferedAppendableFastAppendable(
    private val wrapped: Appendable,
    private val buffer: StringBuilder = StringBuilder()
) : _Private_FastAppendable, Flushable, Closeable, Appendable by buffer {

    override fun appendAscii(c: Char) { append(c) }
    override fun appendAscii(csq: CharSequence?) { append(csq) }
    override fun appendAscii(csq: CharSequence?, start: Int, end: Int) { append(csq, start, end) }
    override fun appendUtf16(c: Char) { append(c) }

    override fun appendUtf16Surrogate(leadSurrogate: Char, trailSurrogate: Char) {
        append(leadSurrogate)
        append(trailSurrogate)
    }

    override fun close() {
        flush()
        if (wrapped is Closeable) wrapped.close()
    }

    override fun flush() {
        wrapped.append(buffer)
        if (wrapped is Flushable) wrapped.flush()
        buffer.setLength(0)
    }
}
