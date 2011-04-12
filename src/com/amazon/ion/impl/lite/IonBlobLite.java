// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonType;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.Base64Encoder;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class IonBlobLite
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

    /**
     * makes a copy of this IonBlob including an independant
     * copy of the bytes. It also calls IonValueImpl to copy
     * the annotations and the field name if appropriate.
     * The symbol table is not copied as the value is fully
     * materialized and the symbol table is unnecessary.
     */
    @Override
    public IonBlobLite clone()
    {
        IonBlobLite clone = new IonBlobLite(this._context.getSystemLite(), false);

        clone.copyFrom(this);

        return clone;
    }

    /**
     * Implements {@link Object#hashCode()} consistent with equals.
     *
     * @return  An int, consistent with the contracts for
     *          {@link Object#hashCode()} and {@link Object#equals(Object)}.
     */
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

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
