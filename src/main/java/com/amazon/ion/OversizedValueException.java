// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion;

/**
 * An error thrown when the user requests an IonReader to parse a scalar value that
 * exceeds the reader's configured maximum buffer size. This is never thrown from
 * {@link IonReader#next()}; oversized values encountered during next() are
 * skipped. This may be thrown from any {@link IonReader} `*value()` method,
 * but only when a maximum buffer size is specified, and only when incremental
 * reading is disabled. When incremental reading is enabled, all oversized values
 * will be detected during `IonReader.next()`, as calling that method at the top
 * level causes the reader to attempt to buffer the entire top-level value. This
 * custom exception type exists because several IonReader `*value()` interface
 * methods return primitives, leaving no way to alert the user to failure.
 * This exception is recoverable; after catching this exception, the user may
 * call `IonReader.next()` to position the reader on the next value in the
 * stream and continue processing.
 */
public class OversizedValueException extends IonException {

    public OversizedValueException() {
        super("Attempted to parse a scalar value that exceeded the reader's maximum buffer size.");
    }
}
