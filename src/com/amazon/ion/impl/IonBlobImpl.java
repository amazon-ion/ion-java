// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;


import com.amazon.ion.IonBlob;
import com.amazon.ion.IonType;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;
import java.io.InputStream;

/**
 * Implements the Ion <code>blob</code> type.
 */
public final class IonBlobImpl
    extends IonLobImpl
    implements IonBlob
{

    static final int NULL_BLOB_TYPEDESC =
         IonConstants.makeTypeDescriptor(IonConstants.tidBlob,
                                         IonConstants.lnIsNullAtom);

    /**
     * Constructs a <code>null.blob</code> element.
     */
    public IonBlobImpl(IonSystemImpl system)
    {
        super(system, NULL_BLOB_TYPEDESC);
        _hasNativeValue = true; // Since this is null
    }

    /**
     * Constructs a binary-backed element.
     */
    public IonBlobImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
        assert pos_getType() == IonConstants.tidBlob;
    }

    /**
     * makes a copy of this IonBlob including an independant
     * copy of the bytes. It also calls IonValueImpl to copy
     * the annotations and the field name if appropriate.
     * The symbol table is not copied as the value is fully
     * materialized and the symbol table is unnecessary.
     */
    @Override
    public IonBlobImpl clone()
    {
        IonBlobImpl clone = new IonBlobImpl(_system);

        clone.copyFrom(this);

        return clone;
    }

    public IonType getType()
    {
        return IonType.BLOB;
    }


    @Deprecated
    public void appendBase64(Appendable out)
        throws IOException
    {
        printBase64(out);
    }

    public void printBase64(Appendable out)
        throws IOException
    {
        validateThisNotNull();
        InputStream byteStream = newInputStream();
        try
        {
            Base64Encoder.TextStream ts =
                new Base64Encoder.TextStream(byteStream);

            for (;;) {
                int c = ts.read();
                if (c == -1) break;
                out.append((char) c);
            }
        }
        finally
        {
            byteStream.close();
        }
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
