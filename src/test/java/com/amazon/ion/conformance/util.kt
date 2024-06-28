// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.conformance

import com.amazon.ion.*
import com.amazon.ion.system.*
import com.amazon.ionelement.api.AnyElement
import com.amazon.ionelement.api.IonElementLoaderOptions
import com.amazon.ionelement.api.SeqElement
import java.io.File

val ION: IonSystem = IonSystemBuilder.standard().build()

val ELEMENT_LOADER_OPTIONS = IonElementLoaderOptions(includeLocationMeta = true)

val ION_CONFORMANCE_DIR = File("ion-tests/conformance")

val TEST_CATALOG_DIR = File("ion-tests/catalog")

/**
 * Catalog for conformance tests.
 */
val ION_CONFORMANCE_TEST_CATALOG = SimpleCatalog().apply {
    TEST_CATALOG_DIR.walk()
        .filter { it.isFile && it.extension == "ion" }
        .onEach { println(it.absolutePath) }
        .forEach { file ->
            file.inputStream()
                .let(ION::newReader)
                .use { r -> while (r.next() != null) putTable(ION.newSharedSymbolTable(r, true)) }
        }
}

/**
 * Gets the first value of a [SeqElement].
 * Throws an exception if the first value is not text.
 */
val SeqElement.head: String
    get() = values.first().textValue

/**
 * Gets all elements of a [SeqElement], except for [head].
 */
val SeqElement.tail: List<AnyElement>
    get() = tailFrom(1)

/**
 * Gets the tail of a [SeqElement], starting with position [i].
 */
fun SeqElement.tailFrom(i: Int) = values.subList(i, size)

/**
 * Join a list of [ByteArray] into a single [ByteArray]
 */
fun List<ByteArray>.joinToByteArray(): ByteArray {
    val size = sumOf { it.size }
    var offset = 0
    val combined = ByteArray(size)
    forEach {
        it.copyInto(combined, offset)
        offset += it.size
    }
    return combined
}
