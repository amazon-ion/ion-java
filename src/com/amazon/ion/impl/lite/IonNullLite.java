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

import com.amazon.ion.IonNull;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;


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
    IonValueLite shallowClone(IonContext context)
    {
        return new IonNullLite(this, context);
    }

    @Override
    public IonNullLite clone()
    {
        return (IonNullLite) shallowClone(ContainerlessContext.wrap(getSystem()));
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
    int hashSignature() {
        return HASH_SIGNATURE;
    }

    @Override
    public int scalarHashCode() {
        return hashTypeAnnotations(HASH_SIGNATURE);
    }

}
