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

import static com.amazon.ion.impl._Private_Utils.isNonSymbolScalar;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.util.IonStreamUtils;
import java.io.IOException;


class IonWriterUserBinary
    extends IonWriterUser
    implements _Private_ListWriter
{
    /**
     * This is null if the writer is not stream copy optimized.
     */
    private final _Private_SymtabExtendsCache mySymtabExtendsCache;

    /**
     * This is null if the writer is not stream copy optimized.
     */
    private final _Private_ByteTransferSink myCopySink;

    // If we wanted to we could keep an extra reference to the
    // system writer which was correctly typed as an
    // IonBinaryWriter and avoid the casting in the 3 "overridden"
    // methods.  However those are sufficiently expensive that
    // the cost of the cast should be lost in the noise.

    IonWriterUserBinary(_Private_IonBinaryWriterBuilder options,
                        IonWriterSystemBinary           systemWriter)
    {
        super(options.getCatalog(),
              options.getSymtabValueFactory(),
              systemWriter,
              options.buildContextSymbolTable());

        if (options.isStreamCopyOptimized())
        {
            mySymtabExtendsCache = new _Private_SymtabExtendsCache();
            myCopySink = new _Private_ByteTransferSink()
            {
                public void writeBytes(byte[] data, int off, int len) throws IOException
                {
                    ((IonWriterSystemBinary) _current_writer).writeRaw(data, off, len);
                }
            };
        }
        else
        {
            mySymtabExtendsCache = null;
            myCopySink = null;
        }
    }


    @Override
    public boolean isStreamCopyOptimized()
    {
        return mySymtabExtendsCache != null;
    }

    @Override
    public void writeValue(IonReader reader)
        throws IOException
    {
        // If reader is not on a value, type is null, and NPE will be thrown
        // by calls below
        IonType type = reader.getType();

        // See if we can copy bytes directly from the source. This test should
        // only happen at the outermost call, not recursively down the tree.

        if (isStreamCopyOptimized() &&
            _current_writer instanceof IonWriterSystemBinary)
        {
            _Private_ByteTransferReader transfer =
                reader.asFacet(_Private_ByteTransferReader.class);

            if (transfer != null &&
                (isNonSymbolScalar(type) ||
                 mySymtabExtendsCache.symtabsCompat(getSymbolTable(),
                                                    reader.getSymbolTable())))
            {
                // TODO amazon-ion/ion-java/issues/16 Doesn't copy annotations or field names.
                transfer.transferCurrentValue(myCopySink);
                return;
            }
        }

        // From here on, we won't call back into this method, so we won't
        // bother doing all those checks again.
        writeValueRecursively(reader);
    }


    public void writeBoolList(boolean[] values) throws IOException
    {
        IonStreamUtils.writeBoolList(_current_writer, values);
    }


    public void writeFloatList(float[] values) throws IOException
    {
        IonStreamUtils.writeFloatList(_current_writer, values);
    }


    public void writeFloatList(double[] values) throws IOException
    {
        IonStreamUtils.writeFloatList(_current_writer, values);
    }


    public void writeIntList(byte[] values) throws IOException
    {
        IonStreamUtils.writeIntList(_current_writer, values);
    }


    public void writeIntList(short[] values) throws IOException
    {
        IonStreamUtils.writeIntList(_current_writer, values);
    }


    public void writeIntList(int[] values) throws IOException
    {
        IonStreamUtils.writeIntList(_current_writer, values);
    }


    public void writeIntList(long[] values) throws IOException
    {
        IonStreamUtils.writeIntList(_current_writer, values);
    }


    public void writeStringList(String[] values) throws IOException
    {
        IonStreamUtils.writeStringList(_current_writer, values);
    }
}
