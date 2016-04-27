/*
 * Copyright 2015-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl.bin;

import java.io.IOException;
import java.math.BigInteger;
import software.amazon.ion.Decimal;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Timestamp;
import software.amazon.ion.impl.PrivateByteTransferReader;
import software.amazon.ion.impl.PrivateByteTransferSink;
import software.amazon.ion.impl.PrivateIonWriter;
import software.amazon.ion.impl.PrivateSymtabExtendsCache;
import software.amazon.ion.impl.PrivateUtils;

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
                switch (reader.getIntegerSize())
                {
                    case INT:
                        final int intValue = reader.intValue();
                        writeInt(intValue);
                        break;
                    case LONG:
                        final long longValue = reader.longValue();
                        writeInt(longValue);
                        break;
                    case BIG_INTEGER:
                        final BigInteger bigIntegerValue = reader.bigIntegerValue();
                        writeInt(bigIntegerValue);
                        break;
                    default:
                        throw new IllegalStateException();
                }
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
