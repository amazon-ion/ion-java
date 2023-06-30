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
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.Timestamp;
import com.amazon.ion.UnknownSymbolException;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Iterator;
import java.util.Queue;

import static com.amazon.ion.IonCursor.Event.NEEDS_DATA;

/**
 * Provides a system-level view of an Ion stream. This differs from the application-level view in that system values
 * (IVMs and symbol tables) are surfaced as if they were user values and symbols are not mapped, except for symbols
 * that occur within the system symbol table. Because this is primarily intended to be used for debugging and is not
 * exposed via public interfaces, it is not currently considered performance-critical, and no continuable equivalent
 * is provided.
 */
class IonReaderNonContinuableSystem implements IonReader {

    protected final IonReaderContinuableCore reader;
    private IonType type = null;
    private IonType typeAfterIvm = null;
    private final Queue<Integer> pendingIvmSids = new ArrayDeque<>(1);
    private int pendingIvmSid = -1;

    /**
     * Constructs a new non-continuable system-level reader over the given continuable reader.
     * @param reader the reader to wrap.
     */
    IonReaderNonContinuableSystem(IonReaderContinuableCore reader) {
        this.reader = reader;
        reader.registerIvmNotificationConsumer((x, y) -> {
            pendingIvmSids.add(SystemSymbols.ION_1_0_SID); // TODO generalize for Ion 1.1
        });
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("Not implemented -- use `next() != null`");
    }

    /**
     * Consumes an IVM that was encountered before the next value, presenting the IVM as a symbol value. If an IVM
     * was emitted on the last invocation, restores the next value to be presented to the user.
     * @return true if a value is ready to be presented to the user; otherwise, false.
     */
    private boolean handleIvm() {
        Integer ivmSid = pendingIvmSids.poll();
        if (ivmSid != null) {
            // An IVM has been found between values.
            if (typeAfterIvm == null) {
                // Only save the type of the next user value the first time an IVM is encountered before that value.
                typeAfterIvm = type;
            }
            // For consistency with the legacy implementation, the system reader surfaces IVMs as symbol values.
            type = IonType.SYMBOL;
            pendingIvmSid = ivmSid;
            return true;
        } else if (pendingIvmSid != -1) {
            // All preceding IVMs have been surfaced. Restore the value that follows.
            pendingIvmSid = -1;
            type = typeAfterIvm;
            typeAfterIvm = null;
            return true;
        }
        return false;
    }

    @Override
    public IonType next() {
        if (handleIvm()) {
            // This happens if there were multiple IVMs between top-level values. They will be drained from the queue
            // one-by-one on each call to next().
            return type;
        }
        if (reader.nextValue() == NEEDS_DATA) {
            if (handleIvm()) {
                // This happens if one or more IVMs occurs before the end of the stream.
                return type;
            }
            reader.endStream();
            type = null;
        } else {
            type = reader.getType();
            handleIvm(); // Handles the case where one or more IVMs occurred before the value.
        }
        return type;
    }

    @Override
    public void stepIn() {
        reader.stepIntoContainer();
        type = null;
    }

    @Override
    public void stepOut() {
        reader.stepOutOfContainer();
        type = null;
    }

    @Override
    public int getDepth() {
        return reader.getDepth();
    }

    @Override
    public IonType getType() {
        return type;
    }

    /**
     * Prepares a scalar value to be parsed by ensuring it is present in the buffer.
     */
    protected void prepareScalar() {
        IonCursor.Event event = reader.getCurrentEvent();
        if (event == IonCursor.Event.VALUE_READY) {
            return;
        }
        if (event != IonCursor.Event.START_SCALAR) {
            // Note: existing tests expect IllegalStateException in this case.
            throw new IllegalStateException("Reader is not positioned on a scalar value.");
        }
        if (reader.fillValue() != IonCursor.Event.VALUE_READY) {
            throw new IonException("Unexpected EOF.");
        }
    }

    @Override
    public IntegerSize getIntegerSize() {
        if (getType() != IonType.INT) {
            return null;
        }
        prepareScalar();
        return reader.getIntegerSize();
    }

    @Override
    public boolean isNullValue() {
        return pendingIvmSid == -1 && reader.isNullValue();
    }

    @Override
    public boolean isInStruct() {
        return reader.isInStruct();
    }

    @Override
    public boolean booleanValue() {
        prepareScalar();
        return reader.booleanValue();
    }

    @Override
    public int intValue() {
        prepareScalar();
        return reader.intValue();
    }

    @Override
    public long longValue() {
        prepareScalar();
        return reader.longValue();
    }

    @Override
    public BigInteger bigIntegerValue() {
        prepareScalar();
        return reader.bigIntegerValue();
    }

    @Override
    public double doubleValue() {
        prepareScalar();
        return reader.doubleValue();
    }

    @Override
    public BigDecimal bigDecimalValue() {
        prepareScalar();
        return reader.bigDecimalValue();
    }

    @Override
    public Decimal decimalValue() {
        prepareScalar();
        return reader.decimalValue();
    }

    @Override
    public Date dateValue() {
        prepareScalar();
        return reader.dateValue();
    }

    @Override
    public Timestamp timestampValue() {
        prepareScalar();
        return reader.timestampValue();
    }

    @Override
    public String stringValue() {
        if (pendingIvmSid != -1) {
            return getSymbolTable().findKnownSymbol(pendingIvmSid);
        }
        prepareScalar();
        String value;
        if (type == IonType.SYMBOL) {
            int sid = reader.symbolValueId();
            value = getSymbolTable().findKnownSymbol(sid);
            if (value == null) {
                throw new UnknownSymbolException(sid);
            }
        } else {
            value = reader.stringValue();
        }
        return value;
    }

    @Override
    public int byteSize() {
        prepareScalar();
        return reader.byteSize();
    }

    @Override
    public byte[] newBytes() {
        prepareScalar();
        return reader.newBytes();
    }

    @Override
    public int getBytes(byte[] buffer, int offset, int len) {
        prepareScalar();
        return reader.getBytes(buffer, offset, len);
    }

    @Override
    public <T> T asFacet(Class<T> facetType) {
        return null; // The system-level reader has no facets.
    }

    @Override
    public SymbolTable getSymbolTable() {
        // TODO generalize for Ion 1.1
        return SharedSymbolTable.getSystemSymbolTable(reader.getIonMajorVersion());
    }

    @Override
    public String[] getTypeAnnotations() {
        if (pendingIvmSid != -1 || !reader.hasAnnotations()) {
            return _Private_Utils.EMPTY_STRING_ARRAY;
        }
        int[] annotationIds = reader.getAnnotationIds();
        String[] annotations = new String[annotationIds.length];
        SymbolTable symbolTable = getSymbolTable();
        for (int i = 0; i < annotationIds.length; i++) {
            int sid = annotationIds[i];
            String annotation = symbolTable.findKnownSymbol(sid);
            if (annotation == null) {
                throw new UnknownSymbolException(sid);
            }
            annotations[i] = annotation;
        }
        return annotations;
    }

    @Override
    public SymbolToken[] getTypeAnnotationSymbols() {
        if (pendingIvmSid != -1 || !reader.hasAnnotations()) {
            return SymbolToken.EMPTY_ARRAY;
        }
        int[] annotationIds = reader.getAnnotationIds();
        SymbolToken[] annotationSymbolTokens = new SymbolToken[annotationIds.length];
        SymbolTable symbolTable = getSymbolTable();
        for (int i = 0; i < annotationIds.length; i++) {
            int sid = annotationIds[i];
            annotationSymbolTokens[i] = new SymbolTokenWithImportLocation(symbolTable.findKnownSymbol(sid), sid, null);
        }
        return annotationSymbolTokens;
    }

    @Override
    public Iterator<String> iterateTypeAnnotations() {
        if (pendingIvmSid != -1 || !reader.hasAnnotations()) {
            return _Private_Utils.emptyIterator();
        }
        return _Private_Utils.stringIterator(getTypeAnnotations());
    }

    @Override
    public int getFieldId() {
        return reader.getFieldId();
    }

    @Override
    public String getFieldName() {
        int sid = reader.getFieldId();
        if (sid < 0) {
            return null;
        }
        String name = getSymbolTable().findKnownSymbol(sid);
        if (name == null) {
            throw new UnknownSymbolException(sid);
        }
        return name;
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        int sid = reader.getFieldId();
        if (sid < 0) {
            return null;
        }
        return new SymbolTokenWithImportLocation(getSymbolTable().findKnownSymbol(sid), sid, null);
    }

    @Override
    public SymbolToken symbolValue() {
        int sid;
        if (pendingIvmSid != -1) {
            sid = pendingIvmSid;
        } else {
            prepareScalar();
            sid = reader.symbolValueId();
        }
        return new SymbolTokenWithImportLocation(getSymbolTable().findKnownSymbol(sid), sid, null);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
