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

package com.amazon.ion.impl.bin;

import com.amazon.ion.IonBinaryWriter;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * Adapts arbitrary {@link IonWriter} implementations to an {@link IonBinaryWriter} instances.
 * <p>
 * This class is provided as a shim for compatibility to allow binary writer implementations to be adaptable to
 * the legacy interface.
 */
@Deprecated
/*package*/ final class IonBinaryWriterAdapter implements IonBinaryWriter
{
    /**
     * Simple interface for constructing an {@link IonWriter} from an output stream.
     * Essentially used as a polymorphic constructor.
     */
    public interface Factory
    {
        IonWriter create(final OutputStream out) throws IOException;
    }

    /** Internal {@link ByteArrayOutputStream} implementation to get access to the buffer. */
    private static class InternalByteArrayOutputStream extends ByteArrayOutputStream
    {
        public byte[] bytes()
        {
            return buf;
        }
    }

    private final InternalByteArrayOutputStream buffer;
    private final IonWriter delegate;

    public IonBinaryWriterAdapter(final Factory factory) throws IOException
    {
        this.buffer = new InternalByteArrayOutputStream();
        this.delegate = factory.create(buffer);
    }

    /*package*/ IonWriter getDelegate()
    {
        return delegate;
    }

    /*package*/ void reset()
    {
        buffer.reset();
    }

    // Adapter Methods

    public int byteSize()
    {
        return buffer.size();
    }

    public byte[] getBytes() throws IOException
    {
        return buffer.toByteArray();
    }

    public int getBytes(byte[] bytes, int offset, int maxlen) throws IOException
    {
        final int amount = Math.min(maxlen, buffer.size());
        System.arraycopy(buffer.bytes(), 0, bytes, offset, amount);
        return amount;
    }

    public int writeBytes(OutputStream userstream) throws IOException
    {
        buffer.writeTo(userstream);
        return buffer.size();
    }

    // Delegates

    public SymbolTable getSymbolTable()
    {
        return delegate.getSymbolTable();
    }

    public void flush() throws IOException
    {
        delegate.flush();
    }

    public void finish() throws IOException
    {
        delegate.finish();
    }

    public void close() throws IOException
    {
        delegate.close();
    }

    public void setFieldName(String name)
    {
        delegate.setFieldName(name);
    }

    public void setFieldNameSymbol(SymbolToken name)
    {
        delegate.setFieldNameSymbol(name);
    }

    public void setTypeAnnotations(String... annotations)
    {
        delegate.setTypeAnnotations(annotations);
    }

    public void setTypeAnnotationSymbols(SymbolToken... annotations)
    {
        delegate.setTypeAnnotationSymbols(annotations);
    }

    public void addTypeAnnotation(String annotation)
    {
        delegate.addTypeAnnotation(annotation);
    }

    public void stepIn(IonType containerType) throws IOException
    {
        delegate.stepIn(containerType);
    }

    public void stepOut() throws IOException
    {
        delegate.stepOut();
    }

    public boolean isInStruct()
    {
        return delegate.isInStruct();
    }

    // Write Methods

    public void writeValue(IonValue value) throws IOException
    {
        delegate.writeValue(value);
    }

    public void writeValue(IonReader reader) throws IOException
    {
        delegate.writeValue(reader);
    }

    public void writeValues(IonReader reader) throws IOException
    {
        delegate.writeValues(reader);
    }

    public void writeNull() throws IOException
    {
        delegate.writeNull();
    }

    public void writeNull(IonType type) throws IOException
    {
        delegate.writeNull(type);
    }

    public void writeBool(boolean value) throws IOException
    {
        delegate.writeBool(value);
    }

    public void writeInt(long value) throws IOException
    {
        delegate.writeInt(value);
    }

    public void writeInt(BigInteger value) throws IOException
    {
        delegate.writeInt(value);
    }

    public void writeFloat(double value) throws IOException
    {
        delegate.writeFloat(value);
    }

    public void writeDecimal(BigDecimal value) throws IOException
    {
        delegate.writeDecimal(value);
    }

    public void writeTimestamp(Timestamp value) throws IOException
    {
        delegate.writeTimestamp(value);
    }

    public void writeTimestampUTC(Date value) throws IOException
    {
        delegate.writeTimestampUTC(value);
    }

    public void writeSymbol(String content) throws IOException
    {
        delegate.writeSymbol(content);
    }

    public void writeSymbolToken(SymbolToken content) throws IOException
    {
        delegate.writeSymbolToken(content);
    }

    public void writeString(String value) throws IOException
    {
        delegate.writeString(value);
    }

    public void writeClob(byte[] value) throws IOException
    {
        delegate.writeClob(value);
    }

    public void writeClob(byte[] value, int start, int len) throws IOException
    {
        delegate.writeClob(value, start, len);
    }

    public void writeBlob(byte[] value) throws IOException
    {
        delegate.writeBlob(value);
    }

    public void writeBlob(byte[] value, int start, int len) throws IOException
    {
        delegate.writeBlob(value, start, len);
    }

    public <T> T asFacet(Class<T> facetType)
    {
        // This implementation has no facets.
        return null;
    }
}
