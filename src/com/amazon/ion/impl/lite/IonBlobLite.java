// Copyright (c) 2010-2015 Amazon.com, Inc.  All rights reserved.

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
    IonBlobLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonBlobLite(IonBlobLite existing, IonContext context)
    {
        super(existing, context);
    }

    @Override
    IonBlobLite clone(IonContext context)
    {
        return new IonBlobLite(this, context);
    }

    @Override
    public IonBlobLite clone()
    {
        return clone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    int hashCode(SymbolTableProvider symbolTableProvider) {
        return lobHashCode(HASH_SIGNATURE, symbolTableProvider);
    }

    @Override
    public IonType getType()
    {
        return IonType.BLOB;
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
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
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
