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

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.ValueVisitor;
import java.util.Collection;


final class IonSexpLite
    extends IonSequenceLite
    implements IonSexp
{
    private static final int HASH_SIGNATURE =
        IonType.SEXP.toString().hashCode();

    IonSexpLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonSexpLite(IonSexpLite existing, IonContext context)
    {
        super(existing, context);
    }

    /**
     * Constructs a sexp value
     *
     * @param elements
     *   the initial set of child elements.  If <code>null</code>, then the new
     *   instance will have <code>{@link #isNullValue()} == true</code>.
     *
     * @throws ContainedValueException if any value in <code>elements</code>
     *  has <code>{@link IonValue#getContainer()} != null</code>.
     */
    IonSexpLite(ContainerlessContext context,
                Collection<? extends IonValue> elements)
        throws ContainedValueException
    {
        super(context, elements);
    }

    @Override
    public IonSexpLite clone()
    {
        return (IonSexpLite) deepClone();
    }

    @Override
    IonValueLite shallowClone(IonContext context)
    {
        return new IonSexpLite(this, context);
    }

    @Override
    int hashSignature() {
        return HASH_SIGNATURE;
    }

    @Override
    public IonType getType()
    {
        return IonType.SEXP;
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
