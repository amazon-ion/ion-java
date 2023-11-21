/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.ion.impl.macro.ionelement.impl

import com.amazon.ion.IntegerSize
import com.amazon.ion.IonException
import com.amazon.ion.IonReader
import com.amazon.ion.IonType
import com.amazon.ion.OffsetSpan
import com.amazon.ion.SpanProvider
import com.amazon.ion.TextSpan
import com.amazon.ion.impl.macro.ionelement.api.*
import com.amazon.ion.system.IonReaderBuilder

internal class IonElementLoaderImpl(private val options: IonElementLoaderOptions) : IonElementLoader {

    /**
     * Catches an [IonException] occurring in [block] and throws an [IonElementLoaderException] with
     * the current [IonLocation] of the fault, if one is available.  Note that depending on the state of the
     * [IonReader], a location may in fact not be available.
     */
    private inline fun <T> handleReaderException(ionReader: IonReader, crossinline block: () -> T): T {
        try {
            return block()
        } catch (e: IonException) {
            throw IonElementException(
                location = ionReader.currentLocation(),
                description = "IonException occurred, likely due to malformed Ion data (see cause)",
                cause = e
            )
        }
    }

    private fun IonReader.currentLocation(): IonLocation? =
        when {
            // Can't attempt to get a SpanProvider unless we're on a value
            this.type == null -> null
            else -> {
                val spanFacet = this.asFacet(SpanProvider::class.java)
                when (val currentSpan = spanFacet.currentSpan()) {
                    is TextSpan -> IonTextLocation(currentSpan.startLine, currentSpan.startColumn)
                    is OffsetSpan -> IonBinaryLocation(currentSpan.startOffset)
                    else -> null
                }
            }
        }

    override fun loadSingleElement(ionText: String): AnyElement =
        IonReaderBuilder.standard().build(ionText).use { ionReader ->
            loadSingleElement(ionReader)
        }

    override fun loadSingleElement(ionReader: IonReader): AnyElement {
        return handleReaderException(ionReader) {
            ionReader.next()
            loadCurrentElement(ionReader).also {
                ionReader.next()
                require(ionReader.type == null) { "More than a single value was present in the specified IonReader." }
            }
        }
    }

    override fun loadAllElements(ionReader: IonReader): List<AnyElement> {
        return handleReaderException(ionReader) {
            mutableListOf<AnyElement>().also { fields ->
                ionReader.forEachValue { fields.add(loadCurrentElement(ionReader)) }
            }
        }
    }

    override fun loadAllElements(ionText: String): List<AnyElement> =
        IonReaderBuilder.standard().build(ionText).use { ionReader ->
            return ArrayList<AnyElement>().also { list ->
                ionReader.forEachValue {
                    list.add(loadCurrentElement(ionReader))
                }
            }.toList()
        }

    override fun loadCurrentElement(ionReader: IonReader): AnyElement {
        return handleReaderException(ionReader) {
            require(ionReader.type != null) { "The IonReader was not positioned at an element." }

            val valueType = ionReader.type

            val annotations = ionReader.typeAnnotations!!

            val metas = when {
                options.includeLocationMeta -> {
                    val location = ionReader.currentLocation()
                    when {
                        location != null -> metaContainerOf(ION_LOCATION_META_TAG to location)
                        else -> emptyMetaContainer()
                    }
                }
                else -> emptyMetaContainer()
            }

            var element: AnyElement = when {
                ionReader.type == IonType.DATAGRAM -> error("IonElementLoaderImpl does not know what to do with IonType.DATAGRAM")
                ionReader.isNullValue -> ionNull(valueType.toElementType())
                else -> {
                    when {
                        !IonType.isContainer(valueType) -> {
                            when (valueType) {
                                IonType.BOOL -> ionBool(ionReader.booleanValue())
                                IonType.INT -> when (ionReader.integerSize!!) {
                                    IntegerSize.BIG_INTEGER -> {
                                        val bigIntValue = ionReader.bigIntegerValue()
                                        // Ion java's IonReader appears to determine integerSize based on number of bits,
                                        // not on the actual value, which means if we have a padded int that is > 63 bits,
                                        // but who's value only uses <= 63 bits then integerSize is still BIG_INTEGER.
                                        // Compensate for that here...
                                        if (bigIntValue > MAX_LONG_AS_BIG_INT || bigIntValue < MIN_LONG_AS_BIG_INT)
                                            ionInt(bigIntValue)
                                        else {
                                            ionInt(ionReader.longValue())
                                        }
                                    }
                                    IntegerSize.LONG, IntegerSize.INT -> ionInt(ionReader.longValue())
                                }
                                IonType.FLOAT -> ionFloat(ionReader.doubleValue())
                                IonType.DECIMAL -> ionDecimal(ionReader.decimalValue())
                                IonType.TIMESTAMP -> ionTimestamp(ionReader.timestampValue())
                                IonType.STRING -> ionString(ionReader.stringValue())
                                IonType.SYMBOL -> ionSymbol(ionReader.stringValue())
                                IonType.CLOB -> ionClob(ionReader.newBytes())
                                IonType.BLOB -> ionBlob(ionReader.newBytes())
                                else ->
                                    error("Unexpected Ion type for scalar Ion data type ${ionReader.type}.")
                            }
                        }
                        else -> {
                            ionReader.stepIn()
                            when (valueType) {
                                IonType.LIST -> {
                                    ionListOf(loadAllElements(ionReader))
                                }
                                IonType.SEXP -> {
                                    ionSexpOf(loadAllElements(ionReader))
                                }
                                IonType.STRUCT -> {
                                    val fields = mutableListOf<StructField>()
                                    ionReader.forEachValue { fields.add(StructFieldImpl(ionReader.fieldName, loadCurrentElement(ionReader))) }
                                    ionStructOf(fields)
                                }
                                else -> error("Unexpected Ion type for container Ion data type ${ionReader.type}.")
                            }.also {
                                ionReader.stepOut()
                            }
                        }
                    }
                }
            }.asAnyElement()

            if (annotations.any()) {
                element = element._withAnnotations(*annotations)
            }
            if (metas.any()) {
                element = element._withMetas(metas)
            }

            element
        }
    }
}

/**
 * Calls [IonReader.next] and invokes [block] until all values at the current level in the [IonReader]
 * have been exhausted.
 * */
private fun <T> IonReader.forEachValue(block: () -> T) {
    while (this.next() != null) {
        block()
    }
}
