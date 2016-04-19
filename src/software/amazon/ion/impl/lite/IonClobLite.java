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
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import software.amazon.ion.IonClob;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.ValueVisitor;
import software.amazon.ion.impl.PrivateUtils;

final class IonClobLite
    extends IonLobLite
    implements IonClob
{
    static private final int HASH_SIGNATURE =
        IonType.CLOB.toString().hashCode();

    /**
     * Constructs a <code>null.clob</code> element.
     */
    IonClobLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonClobLite(IonClobLite existing, IonContext context)
    {
        super(existing, context);
    }

    @Override
    IonClobLite clone(IonContext context)
    {
        return new IonClobLite(this, context);
    }

    @Override
    public IonClobLite clone()
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
        return IonType.CLOB;
    }


    public Reader newReader(Charset cs)
    {
        InputStream in = newInputStream();
        if (in == null) return null;

        return new InputStreamReader(in, cs);
    }


    public String stringValue(Charset cs)
    {
        // TODO use Charset directly.
        byte[] bytes = getBytes();
        if (bytes == null) return null;

        return PrivateUtils.decode(bytes, cs);
    }

    @Override
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
        throws IOException
    {
        writer.writeClob(getBytesNoCopy());
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
