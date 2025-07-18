// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonType;
import com.amazon.ion.IvmNotificationConsumer;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.macro.EncodingContext;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.function.Consumer;

/**
 * A delegation implementation of {@link IonReaderContinuableCore} that forwards all method calls
 * to another {@link IonReaderContinuableCore} instance.
 */
abstract class DelegatingIonReaderContinuableCore implements IonReaderContinuableCore {

    protected IonReaderContinuableCore delegate = null;

    void setDelegate(IonReaderContinuableCore newDelegate) {
        delegate = newDelegate;
    }

    // IonCursor methods

    @Override
    public Event nextValue() {
        return delegate.nextValue();
    }

    @Override
    public Event stepIntoContainer() {
        return delegate.stepIntoContainer();
    }

    @Override
    public Event stepOutOfContainer() {
        return delegate.stepOutOfContainer();
    }

    @Override
    public Event fillValue() {
        return delegate.fillValue();
    }

    @Override
    public Event getCurrentEvent() {
        return delegate.getCurrentEvent();
    }

    @Override
    public Event endStream() {
        return delegate.endStream();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    // IonReaderContinuableCore methods

    @Override
    public int getDepth() {
        return delegate.getDepth();
    }

    @Override
    public IonType getType() {
        return delegate.getType();
    }

    @Override
    public IonType getEncodingType() {
        return delegate.getEncodingType();
    }

    @Override
    public IntegerSize getIntegerSize() {
        return delegate.getIntegerSize();
    }

    @Override
    public boolean isNullValue() {
        return delegate.isNullValue();
    }

    @Override
    public boolean isInStruct() {
        return delegate.isInStruct();
    }

    @Override
    @Deprecated
    public int getFieldId() {
        return delegate.getFieldId();
    }

    @Override
    public boolean hasFieldText() {
        return delegate.hasFieldText();
    }

    @Override
    public String getFieldText() {
        return delegate.getFieldText();
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        return delegate.getFieldNameSymbol();
    }

    @Override
    @Deprecated
    public void consumeAnnotationTokens(Consumer<SymbolToken> consumer) {
        delegate.consumeAnnotationTokens(consumer);
    }

    @Override
    public boolean booleanValue() {
        return delegate.booleanValue();
    }

    @Override
    public int intValue() {
        return delegate.intValue();
    }

    @Override
    public long longValue() {
        return delegate.longValue();
    }

    @Override
    public BigInteger bigIntegerValue() {
        return delegate.bigIntegerValue();
    }

    @Override
    public double doubleValue() {
        return delegate.doubleValue();
    }

    @Override
    public BigDecimal bigDecimalValue() {
        return delegate.bigDecimalValue();
    }

    @Override
    public Decimal decimalValue() {
        return delegate.decimalValue();
    }

    @Override
    public Date dateValue() {
        return delegate.dateValue();
    }

    @Override
    public Timestamp timestampValue() {
        return delegate.timestampValue();
    }

    @Override
    public String stringValue() {
        return delegate.stringValue();
    }

    @Override
    @Deprecated
    public int symbolValueId() {
        return delegate.symbolValueId();
    }

    @Override
    public boolean hasSymbolText() {
        return delegate.hasSymbolText();
    }

    @Override
    public String getSymbolText() {
        return delegate.getSymbolText();
    }

    @Override
    public SymbolToken symbolValue() {
        return delegate.symbolValue();
    }

    @Override
    public int byteSize() {
        return delegate.byteSize();
    }

    @Override
    public byte[] newBytes() {
        return delegate.newBytes();
    }

    @Override
    public int getBytes(byte[] buffer, int offset, int len) {
        return delegate.getBytes(buffer, offset, len);
    }

    @Override
    public int getIonMajorVersion() {
        return delegate.getIonMajorVersion();
    }

    @Override
    public int getIonMinorVersion() {
        return delegate.getIonMinorVersion();
    }

    @Override
    public void registerIvmNotificationConsumer(IvmNotificationConsumer ivmConsumer) {
        delegate.registerIvmNotificationConsumer(ivmConsumer);
    }

    @Override
    public boolean hasAnnotations() {
        return delegate.hasAnnotations();
    }

    @Override
    public void resetEncodingContext() {
        delegate.resetEncodingContext();
    }

    @Override
    public String getSymbol(int sid) {
        return delegate.getSymbol(sid);
    }

    @Override
    public EncodingContext getEncodingContext() {
        return delegate.getEncodingContext();
    }
}
