// Copyright (c) 2010-2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl._Private_Utils.isNonSymbolScalar;
import static com.amazon.ion.impl._Private_Utils.symtabExtends;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.util.IonStreamUtils;
import java.io.IOException;

/**
 *
 */
class IonWriterUserBinary
    extends IonWriterUser
    implements _Private_ListWriter
{
    /**
     * Cache to reduce unnecessary calls to
     * {@link _Private_Utils#symtabExtends(SymbolTable, SymbolTable)}. This is
     * only used if the writer is stream copy optimized.
     */
    private static class SymtabExtendsCache
    {
        private SymbolTable myWriterSymtab;
        private SymbolTable myReaderSymtab;
        private int myWriterSymtabMaxId;
        private int myReaderSymtabMaxId;
        private boolean myResult;

        private boolean symtabsCompat(SymbolTable writerSymtab,
                                      SymbolTable readerSymtab)
        {
            // If the refs. of both writer's and reader's symtab match and are
            // not modified, skip expensive symtab extends check and return
            // cached result.

            assert writerSymtab != null && readerSymtab != null:
                "writer's and reader's current symtab cannot be null";

            if (myWriterSymtab          == writerSymtab &&
                myReaderSymtab          == readerSymtab &&
                myWriterSymtabMaxId     == writerSymtab.getMaxId() &&
                myReaderSymtabMaxId     == readerSymtab.getMaxId())
            {
                // Not modified, return cached result
                return myResult;
            }

            myResult = symtabExtends(writerSymtab, readerSymtab);

            // Track refs.
            myWriterSymtab = writerSymtab;
            myReaderSymtab = readerSymtab;

            // Track modification
            myWriterSymtabMaxId = writerSymtab.getMaxId();
            myReaderSymtabMaxId = readerSymtab.getMaxId();

            return myResult;
        }
    }

    /**
     * This is null if the writer is not stream copy optimized.
     */
    private final SymtabExtendsCache mySymtabExtendsCache;

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

        mySymtabExtendsCache = (options.isStreamCopyOptimized()
                                    ? new SymtabExtendsCache()
                                    : null);
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
            ByteTransferReader transfer =
                reader.asFacet(ByteTransferReader.class);

            if (transfer != null &&
                (isNonSymbolScalar(type) ||
                 mySymtabExtendsCache.symtabsCompat(getSymbolTable(),
                                                    reader.getSymbolTable())))
            {
                // TODO ION-241 Doesn't copy annotations or field names.
                transfer
                    .transferCurrentValue((IonWriterSystemBinary) _current_writer);

                return;
            }
        }

        // From here on, we won't call back into this method, so we won't
        // bother doing all those checks again.
        writeValueRecursively(type, reader);
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
