// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonReaderBuilder;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * An optionally continuable (i.e., incremental) binary {@link IonReader} implementation. Continuability is enabled
 * using {@code IonReaderBuilder.withIncrementalReadingEnabled(true)}.
 * <p>
 * When continuable reading is enabled, if
 * {@link IonReader#next()} returns {@code null} at the top-level, it indicates that there is not (yet) enough data in
 * the stream to complete a top-level value. The user may wait for more data to become available in the stream and
 * call {@link IonReader#next()} again to continue reading. Unlike the non-incremental reader, the continuable reader
 * will never throw an exception due to unexpected EOF during {@code next()}. If, however, {@link IonReader#close()} is
 * called when an incomplete value is buffered, an {@link IonException} will be raised.
 * </p>
 * <p>
 * There is one caveat with the continuable reader implementation: it must be able to buffer an entire top-level value
 * and any preceding system values (Ion version marker(s) and symbol table(s)) in memory. This means that each value
 * and preceding system values must be no larger than any of the following:
 * <ul>
 * <li>The configured maximum buffer size of the {@link IonBufferConfiguration}.</li>
 * <li>The memory available to the JVM.</li>
 * <li>2GB, because the buffer is held in a Java {@code byte[]}, which is indexed by an {@code int}.</li>
 * </ul>
 * This will not be a problem for the vast majority of Ion streams, as it is
 * rare for a single top-level value or symbol table to exceed a few megabytes in size. However, if the size of the
 * stream's values risk exceeding the available memory, then continuable reading must not be used.
 * </p>
 */
final class IonReaderContinuableTopLevelBinary extends IonReaderContinuableApplicationBinary implements IonReader, _Private_ReaderWriter {

    // True if continuable reading is disabled.
    private final boolean isNonContinuable;

    // True if input is sourced from a non-fixed stream and the reader is non-continuable, meaning that its top level
    // values are not automatically filled during next().
    private final boolean isFillRequired;

    // True if a value is in the process of being filled.
    private boolean isFillingValue = false;

    // The type of value on which the reader is currently positioned.
    private IonType type = null;

    // The SymbolTable that was transferred via the last call to pop_passed_symbol_table.
    private SymbolTable symbolTableLastTransferred = null;

    /**
     * Constructs a new reader from the given input stream.
     * @param builder the builder containing the configuration for the new reader.
     * @param alreadyRead the byte array containing the bytes already read (often the IVM).
     * @param alreadyReadOff the offset into 'alreadyRead` at which the first byte that was already read exists.
     * @param alreadyReadLen the number of bytes already read from `alreadyRead`.
     */
    IonReaderContinuableTopLevelBinary(IonReaderBuilder builder, InputStream inputStream, byte[] alreadyRead, int alreadyReadOff, int alreadyReadLen) {
        super(builder, inputStream, alreadyRead, alreadyReadOff, alreadyReadLen);
        isNonContinuable = !builder.isIncrementalReadingEnabled();
        isFillRequired = isNonContinuable;
    }

    /**
     * Constructs a new reader from the given byte array.
     * @param builder the builder containing the configuration for the new reader.
     * @param data the byte array containing the bytes to read.
     * @param offset the offset into the byte array at which the first byte of Ion data begins.
     * @param length the number of bytes to be read from the byte array.
     */
    IonReaderContinuableTopLevelBinary(IonReaderBuilder builder, byte[] data, int offset, int length) {
        super(builder, data, offset, length);
        isNonContinuable = !builder.isIncrementalReadingEnabled();
        isFillRequired = false;
    }

    @Override
    public SymbolTable pop_passed_symbol_table() {
        SymbolTable currentSymbolTable = getSymbolTable();
        if (currentSymbolTable == symbolTableLastTransferred) {
            // This symbol table has already been returned. Since the contract is that it is a "pop", it should not
            // be returned twice.
            return null;
        }
        symbolTableLastTransferred = currentSymbolTable;
        return symbolTableLastTransferred;
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Advances to the next value and attempts to fill it.
     * @return true if not enough data was available in the stream; otherwise, false.
     */
    private boolean nextAndFill() {
        while (true) {
            if (!isFillingValue) {
                if (nextValue() == IonCursor.Event.NEEDS_DATA) {
                    type = null;
                    return true;
                }
            }
            isFillingValue = true;
            event = fillValue();
            if (event == IonCursor.Event.NEEDS_DATA) {
                type = null;
                return true;
            } else if (event == IonCursor.Event.NEEDS_INSTRUCTION) {
                // The value was skipped for being too large. Get the next one.
                isFillingValue = false;
                continue;
            }
            break;
        }
        isFillingValue = false;
        return false;
    }

    @Override
    public IonType next() {
        if (!isSlowMode || isNonContinuable || parent != null) {
            IonCursor.Event event = super.nextValue();
            if (event == IonCursor.Event.NEEDS_DATA) {
                if (isNonContinuable) {
                    endStream();
                }
                type = null;
                return null;
            } else if (event == IonCursor.Event.NEEDS_INSTRUCTION) {
                // The value was skipped for being too large.
                isFillingValue = false;
            }
        } else {
            if (nextAndFill()) {
                return null;
            }
        }
        type = super.getType();
        return type;
    }

    @Override
    public void stepIn() {
        super.stepIntoContainer();
        type = null;
    }

    @Override
    public void stepOut() {
        super.stepOutOfContainer();
        type = null;
    }

    @Override
    public IonType getType() {
        return type;
    }

    /**
     * Prepares a scalar value to be parsed by ensuring it is present in the buffer.
     */
    private void prepareScalar() {
        if (event == IonCursor.Event.VALUE_READY) {
            return;
        }
        if (event != IonCursor.Event.START_SCALAR) {
            // Note: existing tests expect IllegalStateException in this case.
            throw new IllegalStateException("Reader is not positioned on a scalar value.");
        }
        if (fillValue() == Event.VALUE_READY) {
            return;
        }
        throw new IonException("Unexpected EOF.");
    }

    @Override
    public IntegerSize getIntegerSize() {
        if (type != IonType.INT) {
            return null;
        }
        if (isFillRequired) {
            prepareScalar();
        }
        return super.getIntegerSize();
    }

    @Override
    public boolean booleanValue() {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.booleanValue();
    }

    @Override
    public int intValue() {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.intValue();
    }

    @Override
    public long longValue() {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.longValue();
    }

    @Override
    public BigInteger bigIntegerValue() {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.bigIntegerValue();
    }

    @Override
    public double doubleValue() {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.doubleValue();
    }

    @Override
    public BigDecimal bigDecimalValue() {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.bigDecimalValue();
    }

    @Override
    public Decimal decimalValue() {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.decimalValue();
    }

    @Override
    public Date dateValue() {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.dateValue();
    }

    @Override
    public Timestamp timestampValue() {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.timestampValue();
    }

    @Override
    public String stringValue() {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.stringValue();
    }

    @Override
    public SymbolToken symbolValue() {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.symbolValue();
    }

    @Override
    public int byteSize() {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.byteSize();
    }

    @Override
    public byte[] newBytes() {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.newBytes();
    }

    @Override
    public int getBytes(byte[] buffer, int offset, int len) {
        if (isFillRequired) {
            prepareScalar();
        }
        return super.getBytes(buffer, offset, len);
    }

    @Override
    public <T> T asFacet(Class<T> facetType) {
        return null;
    }

    @Override
    public void close() {
        if (!isNonContinuable) {
            endStream();
        }
        super.close();
    }
}
