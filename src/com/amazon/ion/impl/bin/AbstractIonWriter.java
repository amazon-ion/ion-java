// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.bin;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Date;

/** Common adapter for {@link IonWriter} implementations. */
/*package*/ abstract class AbstractIonWriter implements IonWriter
{
    public final void writeValue(final IonValue value) throws IOException
    {
        final IonSystem ion = value.getSystem();
        final IonReader reader = ion.newReader(value);
        try
        {
            reader.next();
            writeValue(reader);
        } finally
        {
            reader.close();
        }
    }

    public final void writeValue(final IonReader reader) throws IOException
    {
        final IonType type = reader.getType();

        final String fieldName = reader.getFieldName();
        if (fieldName != null)
        {
            setFieldName(fieldName);
        }
        final String[] annotations = reader.getTypeAnnotations();
        if (annotations.length > 0)
        {
            setTypeAnnotations(annotations);
        }
        if (reader.isNullValue())
        {
            writeNull(type);
            return;
        }

        switch (type)
        {
            case BOOL:
                final boolean booleanValue = reader.booleanValue();
                writeBool(booleanValue);
                break;
            case INT:
                final BigInteger bigIntegerValue = reader.bigIntegerValue();
                writeInt(bigIntegerValue);
                break;
            case FLOAT:
                final double doubleValue = reader.doubleValue();
                writeFloat(doubleValue);
                break;
            case DECIMAL:
                final Decimal decimalValue = reader.decimalValue();
                writeDecimal(decimalValue);
                break;
            case TIMESTAMP:
                final Timestamp timestampValue = reader.timestampValue();
                writeTimestamp(timestampValue);
                break;
            case SYMBOL:
                final String symbolValue = reader.stringValue();
                writeSymbol(symbolValue);
                break;
            case STRING:
                final String stringValue = reader.stringValue();
                writeString(stringValue);
                break;
            case CLOB:
                final byte[] clobValue = reader.newBytes();
                writeClob(clobValue);
                break;
            case BLOB:
                final byte[] blobValue = reader.newBytes();
                writeBlob(blobValue);
                break;
            case LIST:
            case SEXP:
            case STRUCT:
                reader.stepIn();
                stepIn(type);
                while (reader.next() != null) {
                    writeValue(reader);
                }
                stepOut();
                reader.stepOut();
                break;
            default:
                throw new IllegalStateException("Unexpected type: " + type);
        }
    }

    public final void writeValues(final IonReader reader) throws IOException
    {
        if (reader.getType() != null)
        {
            writeValue(reader);
        }
        while (reader.next() != null)
        {
            writeValue(reader);
        }
    }

    public final void writeTimestampUTC(final Date value) throws IOException
    {
        writeTimestamp(Timestamp.forDateZ(value));
    }
}
