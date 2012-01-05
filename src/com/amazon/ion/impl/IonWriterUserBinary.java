// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.IonImplUtils.symtabExtends;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueFactory;
import java.io.IOException;

/**
 *
 */
public class IonWriterUserBinary
    extends IonWriterUser
{
    @Deprecated // TODO ION-252 Remove this
    public static final boolean OUR_FAST_COPY_DEFAULT = false;
    @Deprecated // TODO ION-252 Remove this
    public static volatile boolean ourFastCopyEnabled = OUR_FAST_COPY_DEFAULT;

    public final boolean myStreamCopyOptimized;


    // If we wanted to we could keep an extra reference to the
    // system writer which was correctly typed as an
    // IonBinaryWriter and avoid the casting in the 3 "overridden"
    // methods.  However those are sufficiently expensive that
    // the cost of the cast should be lost in the noise.

    IonWriterUserBinary(IonCatalog catalog,
                        ValueFactory symtabValueFactory,
                        IonWriterSystemBinary systemWriter,
                        boolean streamCopyOptimized,
                        SymbolTable... imports)
    {
        super(catalog, symtabValueFactory, systemWriter,
              false /* suppressInitialIvm */, imports);
        myStreamCopyOptimized = streamCopyOptimized;
    }


    @Override
    public void writeValue(IonReader reader)
        throws IOException
    {
        // TODO check reader state, is it on a value?

        IonType type = reader.getType();
        // TODO ION-253 Don't bother optimizing trivial scalars (except symbol?)

        // See if we can copy bytes directly from the source. This test should
        // only happen at the outermost call, not recursively down the tree.

        ByteTransferReader transfer = reader.asFacet(ByteTransferReader.class);

        if ((ourFastCopyEnabled || myStreamCopyOptimized)
            && transfer != null
            && _current_writer instanceof IonWriterSystemBinary
            && symtabExtends(getSymbolTable(), reader.getSymbolTable()))
        {
            IonWriterSystemBinary systemOut =
                (IonWriterSystemBinary) _current_writer;

            // TODO ION-241 Doesn't copy annotations or field names.
            transfer.transferCurrentValue(systemOut);

            return;
        }

        // From here on, we won't call back into this method, so we won't
        // bother doing all those checks again.
        writeValueRecursively(type, reader);
    }
}
