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

import com.amazon.ion.IonClob;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl._Private_Utils;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;


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
    IonValueLite shallowClone(IonContext context)
    {
        return new IonClobLite(this, context);
    }

    @Override
    public IonClobLite clone()
    {
        return (IonClobLite) shallowClone(ContainerlessContext.wrap(getSystem()));
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

        return _Private_Utils.decode(bytes, cs);
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
