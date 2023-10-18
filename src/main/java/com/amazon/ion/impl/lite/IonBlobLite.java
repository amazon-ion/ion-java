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

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl._Private_Utils;
import java.io.IOException;
import java.io.InputStream;


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
    IonValueLite shallowClone(IonContext context)
    {
        return new IonBlobLite(this, context);
    }

    @Override
    public IonBlobLite clone()
    {
        return (IonBlobLite) shallowClone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    int hashSignature() {
        return HASH_SIGNATURE;
    }

    @Override
    int scalarHashCode() {
        return lobHashCode(HASH_SIGNATURE);
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
            _Private_Utils.writeAsBase64(byteStream, out);
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
