// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.bin;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl._Private_ByteTransferReader;
import com.amazon.ion.impl._Private_ByteTransferSink;
import com.amazon.ion.impl._Private_IonWriter;
import com.amazon.ion.impl._Private_SymtabExtendsCache;
import com.amazon.ion.impl._Private_Utils;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Date;

/** Common adapter for binary {@link IonWriter} implementations. */
/*package*/ abstract class AbstractIonWriter implements _Private_IonWriter, _Private_ByteTransferSink
{
    /*package*/ enum WriteValueOptimization
    {
        NONE,
        COPY_OPTIMIZED,
    }

    /** The cache for copy optimization checks--null if not copy optimized. */
    private final _Private_SymtabExtendsCache symtabExtendsCache;

    /*package*/ AbstractIonWriter(final WriteValueOptimization optimization)
    {
        this.symtabExtendsCache = optimization == WriteValueOptimization.COPY_OPTIMIZED
            ? new _Private_SymtabExtendsCache() : null;
    }

    public final void writeValue(final IonValue value) throws IOException
    {
        if (value != null)
        {
            if (value instanceof IonDatagram)
            {
                // XXX this is a hack to make the writer consistent with the backed DOM and flush out an IVM
                finish();
            }
            value.writeTo(this);
        }
    }

    public final void writeValue(final IonReader reader) throws IOException
    {
        final IonType type = reader.getType();

        if (isStreamCopyOptimized())
        {
            final _Private_ByteTransferReader transferReader =
                reader.asFacet(_Private_ByteTransferReader.class);

            if (transferReader != null
                && (_Private_Utils.isNonSymbolScalar(type)
                 || symtabExtendsCache.symtabsCompat(getSymbolTable(), reader.getSymbolTable())))
            {
                // we have something we can pipe over
                transferReader.transferCurrentValue(this);
                return;
            }
        }

        writeValueRecursive(reader);
    }

    public final void writeValueRecursive(final IonReader reader) throws IOException
    {
        final IonType type = reader.getType();

        final SymbolToken fieldName = reader.getFieldNameSymbol();
        if (fieldName != null && !isFieldNameSet() && isInStruct())
        {
            setFieldNameSymbol(fieldName);
        }
        final SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
        if (annotations.length > 0)
        {
            setTypeAnnotationSymbols(annotations);
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

    public final boolean isStreamCopyOptimized()
    {
        return symtabExtendsCache != null;
    }
}
