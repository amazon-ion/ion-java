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
import software.amazon.ion.IonString;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.ValueVisitor;

final class IonStringLite
    extends IonTextLite
    implements IonString
{
    private static final int HASH_SIGNATURE =
        IonType.STRING.toString().hashCode();

    /**
     * Constructs a <code>null.string</code> value.
     */
    IonStringLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonStringLite(IonStringLite existing, IonContext context)
    {
        super(existing, context);
        // no need to set values as these are set at the parent IonTextLite
    }

    @Override
    IonStringLite clone(IonContext parentContext)
    {
        return new IonStringLite(this, parentContext);
    }

    @Override
    public IonStringLite clone()
    {
        return clone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    int hashCode(SymbolTableProvider symbolTableProvider)
    {
        int result = HASH_SIGNATURE;

        if (!isNullValue()) {
            result ^= stringValue().hashCode();
        }

        return hashTypeAnnotations(result, symbolTableProvider);
    }

    @Override
    public IonType getType()
    {
        return IonType.STRING;
    }

    @Override
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
        throws IOException
    {
        writer.writeString(_get_value());
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }

}
