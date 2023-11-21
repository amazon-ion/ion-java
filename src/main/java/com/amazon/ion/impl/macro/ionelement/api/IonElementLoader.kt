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

@file:JvmName("ElementLoader")
package com.amazon.ion.impl.macro.ionelement.api

import com.amazon.ion.IonReader
import com.amazon.ion.impl.macro.ionelement.impl.IonElementLoaderImpl

/**
 * Provides several functions for loading [IonElement] instances.
 *
 * All functions wrap any [com.amazon.ion.IonException] in an instance of [IonElementLoaderException], including the
 * current [IonLocation] if one is available.  Note that depending on the state of the [IonReader], a location
 * may not be available.
 */
public interface IonElementLoader {
    /**
     * Reads a single element from the specified Ion text data.
     *
     * Throws an [IonElementLoaderException] if there are multiple top level elements.
     */
    public fun loadSingleElement(ionText: String): AnyElement

    /**
     * Reads the next element from the specified [IonReader].
     *
     * Expects [ionReader] to be positioned *before* the element to be read.
     *
     * If there are additional elements to be read after reading the next element,
     * throws an [IonElementLoaderException].
     */
    public fun loadSingleElement(ionReader: IonReader): AnyElement

    /**
     * Reads all elements remaining to be read from the [IonReader].
     *
     * Expects [ionReader] to be positioned *before* the first element to be read.
     *
     * Avoid this function when reading large amounts of Ion because a large amount of memory will be consumed.
     * Instead, prefer [IonElementLoaderException].
     */
    public fun loadAllElements(ionReader: IonReader): Iterable<AnyElement>

    /**
     * Reads all of the elements in the specified Ion text data.
     *
     * Avoid this function when reading large amounts of Ion because a large amount of memory will be consumed.
     * Instead, prefer [processAll] or [loadCurrentElement].
     */
    public fun loadAllElements(ionText: String): Iterable<AnyElement>

    /**
     * Reads the current element from the specified [IonReader].  Does not close the [IonReader].
     *
     * Expects [ionReader] to be positioned *on* the element to be read--does not call [IonReader.next].
     *
     * This method can be utilized to fetch and process the elements one by one and can help avoid high memory
     * consumption when processing large amounts of Ion data.
     */
    public fun loadCurrentElement(ionReader: IonReader): AnyElement
}

/**
 * Specifies options for [IonElementLoader].
 *
 * While there is only one property here currently, new properties may be added to this class without breaking
 * source compatibility with prior versions of this library.
 */
public data class IonElementLoaderOptions(
    /**
     * Set to true to cause `IonLocation` to be stored in the [IonElement.metas] collection of all elements loaded.
     *
     * This is `false` by default because it has a performance penalty.
     */
    val includeLocationMeta: Boolean = false
)

/** Creates an [IonElementLoader] implementation with the specified [options]. */
@JvmOverloads
public fun createIonElementLoader(options: IonElementLoaderOptions = IonElementLoaderOptions()): IonElementLoader =
    IonElementLoaderImpl(options)

/** Provides syntactically lighter way of invoking [IonElementLoader.loadSingleElement]. */
@JvmOverloads
public fun loadSingleElement(ionText: String, options: IonElementLoaderOptions = IonElementLoaderOptions()): AnyElement =
    createIonElementLoader(options).loadSingleElement(ionText)

/** Provides syntactically lighter method of invoking [IonElementLoader.loadSingleElement]. */
@JvmOverloads
public fun loadSingleElement(ionReader: IonReader, options: IonElementLoaderOptions = IonElementLoaderOptions()): AnyElement =
    createIonElementLoader(options).loadSingleElement(ionReader)

/** Provides syntactically lighter method of invoking [IonElementLoader.loadAllElements]. */
@JvmOverloads
public fun loadAllElements(ionText: String, options: IonElementLoaderOptions = IonElementLoaderOptions()): Iterable<AnyElement> =
    createIonElementLoader(options).loadAllElements(ionText)

/** Provides syntactically lighter method of invoking [IonElementLoader.loadAllElements]. */
@JvmOverloads
public fun loadAllElements(ionReader: IonReader, options: IonElementLoaderOptions = IonElementLoaderOptions()): Iterable<AnyElement> =
    createIonElementLoader(options).loadAllElements(ionReader)

/** Provides syntactically lighter method of invoking [IonElementLoader.loadAllElements]. */
@JvmOverloads
public fun loadCurrentElement(ionReader: IonReader, options: IonElementLoaderOptions = IonElementLoaderOptions()): AnyElement =
    createIonElementLoader(options).loadCurrentElement(ionReader)
