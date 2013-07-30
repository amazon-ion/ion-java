// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl._Private_LazyDomTrampoline;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
final class IonBlobLite
    extends IonLobLite
    implements IonBlob
{
    private static final int HASH_SIGNATURE =
        IonType.BLOB.toString().hashCode();

    /**
     * Constructs a <code>null.blob</code> element.
     */
    public IonBlobLite(IonSystemLite system, boolean isNull)
    {
        super(system, isNull);
    }

    @Override
    public IonBlobLite clone()
    {
        IonBlobLite clone = new IonBlobLite(this._context.getSystem(), false);

        clone.copyFrom(this);

        return clone;
    }

    @Override
    public int hashCode() {
        return lobHashCode(HASH_SIGNATURE);
    }

    @Override
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
            _Private_LazyDomTrampoline.writeAsBase64(byteStream, out);
        }
        finally
        {
            byteStream.close();
        }
    }

    @Override
    final void writeBodyTo(IonWriter writer)
        throws IOException
    {
        writer.writeBlob(getBytesNoCopy());
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
