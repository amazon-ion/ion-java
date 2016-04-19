/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl.lite;

import java.io.IOException;
import java.io.InputStream;
import software.amazon.ion.IonBlob;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.ValueVisitor;
import software.amazon.ion.impl.PrivateUtils;

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
            PrivateUtils.writeAsBase64(byteStream, out);
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
