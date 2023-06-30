// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion;

import java.io.Closeable;

/**
 * A cursor over a stream of Ion data. NOTE: this interface is not considered part of the public API.
 */
public interface IonCursor extends Closeable {

    /**
     * Conveys the type of event that occurred as a result of operating on the cursor.
     */
    enum Event {

        /**
         * There is not enough data in the stream to complete the requested operation. The operation should be retried
         * when more data is available.
         */
        NEEDS_DATA,

        /**
         * The cursor has completed an operation (e.g. `stepIntoContainer()`) and requires another instruction in order
         * to position itself on the next value.
         */
        NEEDS_INSTRUCTION,

        /**
         * The cursor is positioned on a scalar value.
         */
        START_SCALAR,

        /**
         * The cursor has successfully buffered the entirety of the value on which it is currently positioned, as
         * requested by an invocation of `fillValue()`.
         */
        VALUE_READY,

        /**
         * The cursor is positioned on a container value.
         */
        START_CONTAINER,

        /**
         * The cursor has reached the end of the current container, and requires an instruction to proceed.
         */
        END_CONTAINER
    }

    /**
     * Advances the cursor to the next value, skipping the current value (if any). This method may return:
     * <ul>
     *     <li>NEEDS_DATA, if not enough data is available in the stream</li>
     *     <li>START_SCALAR, if the reader is now positioned on a scalar value</li>
     *     <li>START_CONTAINER, if the reader is now positioned on a container value</li>
     *     <li>END_CONTAINER, if the reader is now positioned at the end of a container value, or</li>
     *     <li>NEEDS_INSTRUCTION, if the reader skipped a value for exceeding the configured maximum buffer size</li>
     * </ul>
     * @return an Event conveying the result of the operation.
     */
    Event nextValue();

    /**
     * Steps the cursor into the container value on which the cursor is currently positioned. This method may return:
     * <ul>
     *     <li>NEEDS_DATA, if not enough data is available in the stream</li>
     *     <li>NEEDS_INSTRUCTION, when successful, indicating that the caller must invoke another method on the cursor
     *     in order to position it on a value
     *     </li>
     * </ul>
     * @return an Event conveying the result of the operation.
     */
    Event stepIntoContainer();

    /**
     * Steps the cursor out of the current container, skipping any values in the container that may follow. This method
     * may return:
     * <ul>
     *     <li>NEEDS_DATA, if not enough data is available in the stream</li>
     *     <li>NEEDS_INSTRUCTION, when successful, indicating that the caller must invoke another method on the cursor
     *     in order to position it on a value
     *     </li>
     * </ul>
     * @return an Event conveying the result of the operation.
     */
    Event stepOutOfContainer();

    /**
     * Buffers the entirety of the value on which the cursor is currently positioned. This method may return:
     * <ul>
     *     <li>NEEDS_DATA, if not enough data is available in the stream</li>
     *     <li>VALUE_READY, if the value was successfully filled</li>
     *     <li>NEEDS_INSTRUCTION, if the value exceeded the configured maximum buffer size and could not be buffered</li>
     * </ul>
     * @return an Event conveying the result of the operation.
     */
    Event fillValue();

    /**
     * Conveys the result of the previous operation.
     * @return an Event conveying the result of the previous operation.
     */
    Event getCurrentEvent();

    /**
     * Causes the cursor to force completion of the value on which it is currently positioned, if any. This method may
     * return:
     * <ul>
     *     <li>NEEDS_DATA, if called when the reader is not positioned on a value, or</li>
     *     <li>START_SCALAR, if ending the stream at the current position unambiguously results in a complete value</li>
     * </ul>
     * Note: in binary Ion, it is never ambiguous whether a value is complete. Therefore, binary IonCursor
     * implementations will only return NEEDS_DATA or throw from this method. In text Ion, ambiguity is possible.
     * For example, the stream <code>true</code> will initially return NEEDS_DATA from {@link #nextValue()} because
     * it cannot yet be known whether the stream contains the boolean value <code>true</code> or, e.g., the symbol
     * value <code>trueNorth</code>. In this example, calling this method will return START_SCALAR, with the cursor
     * positioned on a boolean value.
     * @return an Event conveying the result of the operation.
     * @throws IonException if this method is called below the top level or when the cursor is positioned on an
     * incomplete value.
     */
    Event endStream();
}
