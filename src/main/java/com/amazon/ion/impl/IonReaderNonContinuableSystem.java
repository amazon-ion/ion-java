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
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import static com.amazon.ion.IonCursor.Event.NEEDS_DATA_ORDINAL;

/**
 * Provides a system-level view of an Ion stream. This differs from the application-level view in that system values
 * (IVMs and symbol tables) are surfaced as if they were user values and symbols are not mapped, except for symbols
 * that occur within the system symbol table. Because this is primarily intended to be used for debugging and is not
 * exposed via public interfaces, it is not currently considered performance-critical, and no continuable equivalent
 * is provided.
 */
final class IonReaderNonContinuableSystem implements IonReader {

    private static final SymbolToken IVM_1_0 = new SymbolTokenImpl(SystemSymbols.ION_1_0, SystemSymbols.ION_1_0_SID);
    private static final SymbolToken IVM_1_1 = new SymbolTokenImpl("$ion_1_1", -1);

    /**
     * Represents an IVM that was read that has not yet been exposed as a Symbol value.
     */
    private enum PendingIvm {
        ION_1_0(IVM_1_0),
        ION_1_1(IVM_1_1);

        private final SymbolToken token;
        PendingIvm(SymbolToken symbolToken) {
            token = symbolToken;
        }

        static PendingIvm pendingIvmForVersionOrNull(int major, int minor) {
            if (major != 1) return null;
            if (minor == 0) return ION_1_0;
            if (minor == 1) return ION_1_1;
            return null;
        }
    }

    private final IonReaderContinuableCore reader;
    private IonType type = null;
    private IonType typeAfterIvm = null;
    private final Queue<PendingIvm> pendingIvms = new ArrayDeque<>(1);
    private PendingIvm pendingIvm = null;

    /**
     * Constructs a new non-continuable system-level reader over the given continuable reader.
     * @param reader the reader to wrap.
     */
    IonReaderNonContinuableSystem(IonReaderContinuableCore reader) {
        this.reader = reader;
        reader.registerIvmNotificationConsumer((major, minor) -> {
            PendingIvm ivm = PendingIvm.pendingIvmForVersionOrNull(major, minor);
            if (ivm == null) {
                throw new IllegalStateException("The parser should have already thrown upon encountering this illegal IVM.");
            }
            reader.resetEncodingContext();
            pendingIvms.add(ivm);
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
        PendingIvm nextPendingIvm = pendingIvms.poll();
        if (nextPendingIvm != null) {
            // An IVM has been found between values.
            if (typeAfterIvm == null) {
                // Only save the type of the next user value the first time an IVM is encountered before that value.
                typeAfterIvm = type;
            }
            // For consistency with the legacy implementation, the system reader surfaces IVMs as symbol values.
            type = IonType.SYMBOL;
            pendingIvm = nextPendingIvm;
            return true;
        } else if (pendingIvm != null) {
            // All preceding IVMs have been surfaced. Restore the value that follows.
            pendingIvm = null;
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
        if (reader.nextValue() == NEEDS_DATA_ORDINAL) {
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
    private void prepareScalar() {
        byte event = reader.getCurrentEvent();
        if (event == IonCursor.Event.VALUE_READY_ORDINAL) {
            return;
        }
        if (event != IonCursor.Event.START_SCALAR_ORDINAL) {
            // Note: existing tests expect IllegalStateException in this case.
            throw new IllegalStateException("Reader is not positioned on a scalar value.");
        }
        if (reader.fillValue() != IonCursor.Event.VALUE_READY_ORDINAL) {
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
        return pendingIvm == null && reader.isNullValue();
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
        if (pendingIvm != null) {
            return pendingIvm.token.getText();
        }
        prepareScalar();
        String value;
        if (type == IonType.SYMBOL) {
            if (reader.hasSymbolText()) {
                value = reader.getSymbolText();
            } else {
                int sid = reader.symbolValueId();
                value = getSymbolText(sid);
                if (value == null) {
                    throw new UnknownSymbolException(sid);
                }
            }
        } else {
            value = reader.stringValue();
        }
        return value;
    }

    /**
     * Attempts to match the given symbol ID to text.
     * @param sid the symbol ID.
     * @return the matching symbol text, or null.
     */
    private String getSymbolText(int sid) {
        if (reader.getIonMinorVersion() == 0) {
            // In Ion 1.0, the system symbol table is always available.
            return getSymbolTable().findKnownSymbol(sid);
        }
        return reader.getSymbol(sid);
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
        // TODO generalize for Ion 1.1, whose system symbol table is not necessarily active.
        return SharedSymbolTable.getSystemSymbolTable(reader.getIonMajorVersion());
    }

    @Override
    public String[] getTypeAnnotations() {
        if (pendingIvm != null || !reader.hasAnnotations()) {
            return _Private_Utils.EMPTY_STRING_ARRAY;
        }
        // Note: it is not expected that the system reader is used in performance-sensitive applications; hence,
        // no effort is made optimize the following.
        List<String> annotations = new ArrayList<>();
        reader.consumeAnnotationTokens((token) -> {
            String text = token.getText();
            if (text == null) {
                int sid = token.getSid();
                text = getSymbolText(sid);
                if (text == null) {
                    throw new UnknownSymbolException(sid);
                }
            }
            annotations.add(text);
        });
        return annotations.toArray(_Private_Utils.EMPTY_STRING_ARRAY);
    }

    @Override
    public SymbolToken[] getTypeAnnotationSymbols() {
        if (pendingIvm != null || !reader.hasAnnotations()) {
            return SymbolToken.EMPTY_ARRAY;
        }
        // Note: it is not expected that the system reader is used in performance-sensitive applications; hence,
        // no effort is made optimize the following.
        List<SymbolToken> annotations = new ArrayList<>();
        reader.consumeAnnotationTokens((token) -> {
            String text = token.getText();
            if (text != null) {
                annotations.add(token);
            } else {
                int sid = token.getSid();
                annotations.add(new SymbolTokenImpl(getSymbolText(sid), sid));
            }
        });
        return annotations.toArray(SymbolToken.EMPTY_ARRAY);
    }

    @Override
    public Iterator<String> iterateTypeAnnotations() {
        if (pendingIvm != null || !reader.hasAnnotations()) {
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
        if (reader.hasFieldText()) {
            return reader.getFieldText();
        }
        int sid = reader.getFieldId();
        if (sid < 0) {
            return null;
        }
        String name = getSymbolText(sid);
        if (name == null) {
            throw new UnknownSymbolException(sid);
        }
        return name;
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        String fieldText;
        int sid = -1;
        if (reader.hasFieldText()) {
            fieldText = reader.getFieldText();
        } else {
            sid = reader.getFieldId();
            if (sid < 0) {
                return null;
            }
            fieldText = getSymbolText(sid);
        }
        return new SymbolTokenImpl(fieldText, sid);
    }

    @Override
    public SymbolToken symbolValue() {
        String symbolText;
        int sid = -1;
        if (pendingIvm != null) {
            return pendingIvm.token;
        } else {
            prepareScalar();
            if (reader.hasSymbolText()) {
                symbolText = reader.getSymbolText();
            } else {
                sid = reader.symbolValueId();
                symbolText = getSymbolText(sid);
            }
        }
        return new SymbolTokenImpl(symbolText, sid);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
