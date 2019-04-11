
/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.util._Private_FastAppendable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

class IonWriterSystemTextMarkup extends IonWriterSystemText {

    /**
     * myTypeBeingWritten is used to communicate the type of the data that was
     * requested to be written via a write<Type> to the subsequent methods that
     * need to know the type for calling callbacks.
     * It is not used when writing containers, or separators, since the type of
     * container is passed in by the user, or can be obtained via
     * getContainer().
     */
    private IonType               myTypeBeingWritten;
    private final _Private_MarkupCallback  myCallback;

    public IonWriterSystemTextMarkup(SymbolTable defaultSystemSymtab,
                                     _Private_IonTextWriterBuilder options,
                                     _Private_FastAppendable out)
    {
        this(defaultSystemSymtab, options, out, options.getCallbackBuilder());
    }

    private IonWriterSystemTextMarkup(SymbolTable defaultSystemSymtab,
                                     _Private_IonTextWriterBuilder options,
                                     _Private_FastAppendable out,
                                     _Private_CallbackBuilder builder)
    {
        this(defaultSystemSymtab, options, builder.build(out));
    }

    private IonWriterSystemTextMarkup(SymbolTable defaultSystemSymtab,
                                     _Private_IonTextWriterBuilder options,
                                     _Private_MarkupCallback callback)
    {
        super(defaultSystemSymtab, options, callback.getAppendable());
        myCallback = callback;
    }

    @Override
    void startValue()
        throws IOException
    {
        super.startValue();
        myCallback.beforeValue(myTypeBeingWritten);
    }

    @Override
    void closeValue()
        throws IOException
    {
        myCallback.afterValue(myTypeBeingWritten);
        super.closeValue();
    }

    @Override
    protected void writeFieldNameToken(SymbolToken sym)
        throws IOException
    {
        myCallback.beforeFieldName(myTypeBeingWritten, sym);
        super.writeFieldNameToken(sym);
        myCallback.afterFieldName(myTypeBeingWritten, sym);
    }

    @Override
    protected void writeAnnotations(SymbolToken[] annotations)
        throws IOException
    {
        myCallback.beforeAnnotations(myTypeBeingWritten);
        super.writeAnnotations(annotations);
        myCallback.afterAnnotations(myTypeBeingWritten);
    }

    @Override
    protected void writeAnnotationToken(SymbolToken sym)
        throws IOException
    {
        myCallback.beforeEachAnnotation(myTypeBeingWritten, sym);
        super.writeAnnotationToken(sym);
        myCallback.afterEachAnnotation(myTypeBeingWritten, sym);
    }

    @Override
    protected boolean writeSeparator(boolean followingLongString)
        throws IOException
    {
        // Determine the type of the container.
        IonType containerType = getContainer();
        // Don't set beingWritten, because writeSeparator is called during the
        // writing of another value, and it would mess up the other callbacks

        if (_pending_separator) {
            myCallback.beforeSeparator(containerType);
        }

        followingLongString = super.writeSeparator(followingLongString);

        if (_pending_separator) {
            myCallback.afterSeparator(containerType);
        }
        return followingLongString;
    }

    @Override
    public void stepIn(IonType containerType)
        throws IOException
    {
        myTypeBeingWritten = containerType;
        super.stepIn(containerType);
        myCallback.afterStepIn(containerType);
        myTypeBeingWritten = null;
    }

    @Override
    public void stepOut()
        throws IOException
    {
        myTypeBeingWritten = getContainer();
        myCallback.beforeStepOut(myTypeBeingWritten);
        super.stepOut();
        myTypeBeingWritten = null;
    }

    // ================ Customer Methods for writing Ion data. ================

    @Override
    public void writeBlob(byte[] value, int start, int len)
        throws IOException
    {
        myTypeBeingWritten = IonType.BLOB;
        super.writeBlob(value, start, len);
        myTypeBeingWritten = null;
    }

    @Override
    public void writeBool(boolean value)
        throws IOException
    {
        myTypeBeingWritten = IonType.BOOL;
        super.writeBool(value);
        myTypeBeingWritten = null;
    }

    @Override
    public void writeClob(byte[] value, int start, int len)
        throws IOException
    {
        myTypeBeingWritten = IonType.CLOB;
        super.writeClob(value, start, len);
        myTypeBeingWritten = null;
    }

    @Override
    public void writeDecimal(BigDecimal value)
        throws IOException
    {
        myTypeBeingWritten = IonType.DECIMAL;
        super.writeDecimal(value);
        myTypeBeingWritten = null;
    }

    @Override
    public void writeFloat(double value)
        throws IOException
    {
        myTypeBeingWritten = IonType.FLOAT;
        super.writeFloat(value);
        myTypeBeingWritten = null;
    }

    @Override
    public void writeInt(long value)
        throws IOException
    {
        myTypeBeingWritten = IonType.INT;
        super.writeInt(value);
        myTypeBeingWritten = null;
    }

    @Override
    public void writeInt(BigInteger value)
        throws IOException
    {
        myTypeBeingWritten = IonType.INT;
        super.writeInt(value);
        myTypeBeingWritten = null;
    }

    @Override
    public void writeNull()
        throws IOException
    {
        myTypeBeingWritten = IonType.NULL;
        super.writeNull();
        myTypeBeingWritten = null;
    }

    @Override
    public void writeNull(IonType type)
        throws IOException
    {
        myTypeBeingWritten = type;
        super.writeNull(type);
        myTypeBeingWritten = null;
    }

    @Override
    public void writeString(String value)
        throws IOException
    {
        myTypeBeingWritten = IonType.STRING;
        super.writeString(value);
        myTypeBeingWritten = null;
    }

    @Override
    public void writeSymbolAsIs(String value)
        throws IOException
    {
        myTypeBeingWritten = IonType.SYMBOL;
        super.writeSymbolAsIs(value);
        myTypeBeingWritten = null;
    }

    @Override
    public void writeSymbolAsIs(int value)
        throws IOException
    {
        myTypeBeingWritten = IonType.SYMBOL;
        super.writeSymbolAsIs(value);
        myTypeBeingWritten = null;
    }

    @Override
    public void writeTimestamp(Timestamp value)
        throws IOException
    {
        myTypeBeingWritten = IonType.TIMESTAMP;
        super.writeTimestamp(value);
        myTypeBeingWritten = null;
    }
}
