// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion

import java.io.Closeable
import java.io.IOException

/**
 * An enhancement to an Ion reader that supports macro-aware transcoding.
 */
interface MacroAwareIonReader : Closeable {

    /**
     * Performs a macro-aware transcode of the stream being read by this reader.
     * For Ion 1.0 streams, this functions similarly to providing a system-level
     * [IonReader] to [IonWriter.writeValues]. For Ion 1.1 streams, the transcoded
     * stream will include the same symbol tables, encoding directives, and
     * e-expression invocations as the source stream. In both cases, the
     * transcoded stream will be data-model equivalent to the source stream.
     *
     * The following limitations should be noted:
     * 1. Encoding directives with no effect on the encoding context may be
     *    elided from the transcoded stream. An example would be an encoding
     *    directive that re-exports the existing context but adds no new
     *    macros or new symbols.
     * 2. When transcoding from text to text, comments will not be preserved.
     * 3. Open content in encoding directives (e.g. macro invocations that
     *    expand to nothing) will not be preserved.
     * 4. Granular details of the binary encoding, like inlining vs. interning
     *    for a particular symbol or length-prefixing vs. delimiting for a
     *    particular container, may not be preserved. It is up to the user
     *    to provide a writer configured to match these details if important.
     *
     * To get a [MacroAwareIonReader] use `_Private_IonReaderBuilder.buildMacroAware`.
     * To get a [MacroAwareIonWriter] use [IonEncodingVersion.textWriterBuilder] or
     * [IonEncodingVersion.binaryWriterBuilder].
     * @param writer the writer to which the reader's stream will be transcoded.
     */
    @Throws(IOException::class)
    fun transcodeTo(writer: MacroAwareIonWriter)
}
