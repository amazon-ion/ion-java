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
import com.amazon.ion.impl.PrivateByteTransferReader;
import com.amazon.ion.impl.PrivateByteTransferSink;
import com.amazon.ion.impl.PrivateIonWriter;
import com.amazon.ion.impl.PrivateSymtabExtendsCache;
import com.amazon.ion.impl.PrivateUtils;
import java.io.IOException;
import java.math.BigInteger;

/** Common adapter for binary {@link IonWriter} implementations. */
/*package*/ abstract class AbstractIonWriter implements PrivateIonWriter, PrivateByteTransferSink
{
    /*package*/ enum WriteValueOptimization
    {
        NONE,
        COPY_OPTIMIZED,
    }

    /** The cache for copy optimization checks--null if not copy optimized. */
    private final PrivateSymtabExtendsCache symtabExtendsCache;

    /*package*/ AbstractIonWriter(final WriteValueOptimization optimization)
    {
        this.symtabExtendsCache = optimization == WriteValueOptimization.COPY_OPTIMIZED
            ? new PrivateSymtabExtendsCache() : null;
    }

    public final void writeValue(final IonValue value) throws IOException
    {
        if (value != null)
        {
            if (value instanceof IonDatagram)
            {
                // XXX this is a hack to make the writer consistent with the legacy implementations and flush out an IVM
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
            final PrivateByteTransferReader transferReader =
                reader.asFacet(PrivateByteTransferReader.class);

            if (transferReader != null
                && (PrivateUtils.isNonSymbolScalar(type)
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

        // TODO amznlabs/ion-java#45 make sure the plumbing symbol tokens do the right thing for
        //      different symbol contexts in the reader and this writer

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
                final SymbolToken symbolValue = reader.symbolValue();
                writeSymbolToken(symbolValue);
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

    public final boolean isStreamCopyOptimized()
    {
        return symtabExtendsCache != null;
    }
}
