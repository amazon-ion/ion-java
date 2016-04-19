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
import software.amazon.ion.IonNull;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.ValueVisitor;

final class IonNullLite
    extends IonValueLite
    implements IonNull
{
    private static final int HASH_SIGNATURE =
        IonType.NULL.toString().hashCode();

    protected IonNullLite(ContainerlessContext context)
    {
        super(context, true);
    }

    IonNullLite(IonNullLite existing, IonContext context)
    {
        super(existing, context);
    }

    @Override
    public void accept(final ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }

    @Override
    IonNullLite clone(IonContext context)
    {
        return new IonNullLite(this, context);
    }

    @Override
    public IonNullLite clone()
    {
        return clone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    public IonType getType()
    {
        return IonType.NULL;
    }

    @Override
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
        throws IOException
    {
        writer.writeNull();
    }

    @Override
    public int hashCode(SymbolTableProvider symbolTableProvider) {
        return hashTypeAnnotations(HASH_SIGNATURE, symbolTableProvider);
    }

}
