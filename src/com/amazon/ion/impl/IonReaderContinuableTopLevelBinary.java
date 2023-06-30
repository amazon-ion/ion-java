// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
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
 * An {@link IonReader} adapter to IonReaderContinuableApplicationBinary. Currently non-continuable. Support for
 * continuability will be added.
 */
final class IonReaderContinuableTopLevelBinary extends IonReaderContinuableApplicationBinary implements IonReader, _Private_ReaderWriter {

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

    @Override
    public IonType next() {
        IonCursor.Event event = super.nextValue();
        if (event == IonCursor.Event.NEEDS_DATA) {
            endStream();
            type = null;
            return null;
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
        prepareScalar();
        return super.getIntegerSize();
    }

    @Override
    public boolean booleanValue() {
        prepareScalar();
        return super.booleanValue();
    }

    @Override
    public int intValue() {
        prepareScalar();
        return super.intValue();
    }

    @Override
    public long longValue() {
        prepareScalar();
        return super.longValue();
    }

    @Override
    public BigInteger bigIntegerValue() {
        prepareScalar();
        return super.bigIntegerValue();
    }

    @Override
    public double doubleValue() {
        prepareScalar();
        return super.doubleValue();
    }

    @Override
    public BigDecimal bigDecimalValue() {
        prepareScalar();
        return super.bigDecimalValue();
    }

    @Override
    public Decimal decimalValue() {
        prepareScalar();
        return super.decimalValue();
    }

    @Override
    public Date dateValue() {
        prepareScalar();
        return super.dateValue();
    }

    @Override
    public Timestamp timestampValue() {
        prepareScalar();
        return super.timestampValue();
    }

    @Override
    public String stringValue() {
        prepareScalar();
        return super.stringValue();
    }

    @Override
    public SymbolToken symbolValue() {
        prepareScalar();
        return super.symbolValue();
    }

    @Override
    public int byteSize() {
        prepareScalar();
        return super.byteSize();
    }

    @Override
    public byte[] newBytes() {
        prepareScalar();
        return super.newBytes();
    }

    @Override
    public int getBytes(byte[] buffer, int offset, int len) {
        prepareScalar();
        return super.getBytes(buffer, offset, len);
    }

    @Override
    public <T> T asFacet(Class<T> facetType) {
        return null;
    }
}
