// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
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
                // XXX this is a hack to make the writer consistent with legacy implementations and flush out an IVM
                finish();
            }
            value.writeTo(this);
        }
    }

    public final void writeValue(final IonReader reader) throws IOException
    {
        final IonType type = reader.getType();

        if (isStreamCopyOptimized() && reader instanceof _Private_ByteTransferReader)
        {
            if (_Private_Utils.isNonSymbolScalar(type)
                || symtabExtendsCache.symtabsCompat(getSymbolTable(), reader.getSymbolTable()))
            {
                // we have something we can pipe over
                if (((_Private_ByteTransferReader) reader).transferCurrentValue(this)) {
                    return;
                }
            }
        }

        writeValueRecursive(reader);
    }

    /**
     * Performs a depth-first (recursive-like) traversal of the IonReader's current value, writing all values and
     * annotations encountered during the traversal. This method is not implemented using recursion.
     *
     * @param reader       The IonReader that will provide a value to write.
     * @throws IOException if either the provided IonReader or this writer's underlying OutputStream throw an
     *                     IOException.
     * @throws IllegalStateException if this writer is inside a struct but the IonReader is not.
     */
    public final void writeValueRecursive(final IonReader reader) throws IOException
    {
        // The IonReader does not need to be at the top level (getDepth()==0) when the function is called.
        // We take note of its initial depth so we can avoid advancing the IonReader beyond the starting value.
        int startingDepth = getDepth();

        // The IonReader will be at `startingDepth` when the function is first called and then again when we
        // have finished traversing all of its children. This boolean tracks which of those two states we are
        // in when `getDepth() == startingDepth`.
        boolean alreadyProcessedTheStartingValue = false;

        // The IonType of the IonReader's current value.
        IonType type;

        while (true) {
            // Each time we reach the top of the loop we are in one of three states:
            // 1. We have not yet begun processing the starting value.
            // 2. We are currently traversing the starting value's children.
            // 3. We have finished processing the starting value.
            if (getDepth() == startingDepth) {
                // The IonReader is at the starting depth. We're either beginning our traversal or finishing it.
                if (alreadyProcessedTheStartingValue) {
                    // We're finishing our traversal.
                    break;
                }
                // We're beginning our traversal. Don't advance the cursor; instead, use the current
                // value's IonType.
                type = reader.getType();
                // We've begun processing the starting value.
                alreadyProcessedTheStartingValue = true;
            } else {
                // We're traversing the starting value's children (that is: values at greater depths). We need to
                // advance the cursor by calling next().
                type = reader.next();
            }

            if (type == null) {
                // There are no more values at this level. If we're at the starting level, we're done.
                if (getDepth() == startingDepth) {
                    break;
                }
                // Otherwise, step out once and then try to move forward again.
                reader.stepOut();
                stepOut();
                continue;
            }

            final SymbolToken fieldName = reader.getFieldNameSymbol();
            if (fieldName != null && !isFieldNameSet() && isInStruct()) {
                setFieldNameSymbol(fieldName);
            }
            final SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
            if (annotations.length > 0) {
                setTypeAnnotationSymbols(annotations);
            }
            if (reader.isNullValue()) {
                writeNull(type);
                continue;
            }

            switch (type) {
                case BOOL:
                    final boolean booleanValue = reader.booleanValue();
                    writeBool(booleanValue);
                    break;
                case INT:
                    switch (reader.getIntegerSize()) {
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
                    final SymbolToken symbolToken = reader.symbolValue();
                    writeSymbolToken(symbolToken);
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
                    break;
                default:
                    throw new IllegalStateException("Unexpected type: " + type);
            }
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

    @SuppressWarnings("deprecation")
    public <T> T asFacet(Class<T> facetType)
    {
        if (facetType == _Private_IonManagedWriter.class)
        {
            return facetType.cast(this);
        }
        return null; // Consistent with readers' behavior when requested facet isn't supported
    }

    /**
     * Writes a portion of the byte array out as an IonString value.  This
     * copies the portion of the byte array that is written.
     *
     * @param data bytes to be written.
     * May be {@code null} to represent {@code null.string}.
     * @param offset offset of the first byte in value to write
     * @param length number of bytes to write from value
     * @see IonWriter#writeClob(byte[], int, int)
     * @see IonWriter#writeBlob(byte[], int, int)
     */
    public abstract void writeString(byte[] data, int offset, int length)
        throws IOException;

}
