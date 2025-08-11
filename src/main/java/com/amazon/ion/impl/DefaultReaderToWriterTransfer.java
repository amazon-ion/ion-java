// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.UnknownSymbolException;

import java.io.IOException;

public class DefaultReaderToWriterTransfer {
    private DefaultReaderToWriterTransfer() {}

    /**
     * @throws UnknownSymbolException if the text of the field name is
     *  unknown.
     */
    private static void write_value_field_name_helper(IonReader reader, IonWriter writer)
    {
        if (writer.isInStruct() && !writer.isFieldNameSet())
        {
            SymbolToken tok = reader.getFieldNameSymbol();
            if (tok == null)
            {
                throw new IllegalStateException("Field name not set");
            }

            writer.setFieldNameSymbol(tok);
        }
    }

    private static void write_value_annotations_helper(IonReader reader, IonWriter writer)
    {
        SymbolToken[] a = reader.getTypeAnnotationSymbols();
        // At present, we must always call this, even when the list is empty,
        // because local symtab diversion leaves the $ion_symbol_table
        // dangling on the system writer!  TODO fix that, it's broken.
        writer.setTypeAnnotationSymbols(a);
    }

    /**
     * Overrides can optimize special cases.
     */
    public static void writeValue(IonReader reader, IonWriter writer) throws IOException
    {
        writeValueRecursively(reader, writer);
    }

    /**
     * Writes the provided IonReader's current value including any annotations. This function will not advance the
     * IonReader beyond the end of the current value; users wishing to continue using the IonReader at the current
     * depth will need to call {@link IonReader#next()} again.
     *
     * - If the IonReader is not positioned over a value (for example: because it is at the beginning or end of a
     *   stream), then this function does nothing.
     * - If the current value is a container, this function will visit all of its child values and write those too,
     *   advancing the IonReader to the end of the container in the process.
     * - If both this writer and the IonReader are in a struct, the writer will write the current value's field name.
     * - If the writer is not in a struct but the reader is, the writer will ignore the current value's field name.
     * - If the writer is in a struct but the IonReader is not, this function throws an IllegalStateException.
     *
     * @param reader       The IonReader that will provide a value to write.
     * @throws IOException if either the provided IonReader or this writer's underlying OutputStream throw an
     *                     IOException.
     * @throws IllegalStateException if this writer is inside a struct but the IonReader is not.
     */
    static void writeValueRecursively(IonReader reader, IonWriter writer) throws IOException
    {
        // The IonReader does not need to be at the top level (getDepth()==0) when the function is called.
        // We take note of its initial depth so we can avoid advancing the IonReader beyond the starting value.
        int startingDepth = reader.getDepth();

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
            if (reader.getDepth() == startingDepth) {
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
                if (reader.getDepth() == startingDepth) {
                    break;
                }
                // Otherwise, step out once and then try to move forward again.
                reader.stepOut();
                writer.stepOut();
                continue;
            }

            // We found a value. Write out its field name and annotations, if any.
            write_value_field_name_helper(reader, writer);
            write_value_annotations_helper(reader, writer);

            if (reader.isNullValue()) {
                writer.writeNull(type);
                continue;
            }

            switch (type) {
                case NULL:
                    // The isNullValue() check above will handle this.
                    throw new IllegalStateException("isNullValue() was false but IonType was NULL.");
                case BOOL:
                    writer.writeBool(reader.booleanValue());
                    break;
                case INT:
                    writer.writeInt(reader.bigIntegerValue());
                    break;
                case FLOAT:
                    writer.writeFloat(reader.doubleValue());
                    break;
                case DECIMAL:
                    writer.writeDecimal(reader.decimalValue());
                    break;
                case TIMESTAMP:
                    writer.writeTimestamp(reader.timestampValue());
                    break;
                case STRING:
                    writer.writeString(reader.stringValue());
                    break;
                case SYMBOL:
                    writer.writeSymbolToken(reader.symbolValue());
                    break;
                case BLOB:
                    writer.writeBlob(reader.newBytes());
                    break;
                case CLOB:
                    writer.writeClob(reader.newBytes());
                    break;
                case STRUCT: // Intentional fallthrough
                case LIST:   // Intentional fallthrough
                case SEXP:
                    reader.stepIn();
                    writer.stepIn(type);
                    break;
                default:
                    throw new IllegalStateException("Unknown value type: " + type);
            }
        }
    }
}
