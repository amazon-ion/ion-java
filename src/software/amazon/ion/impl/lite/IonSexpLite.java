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

import java.util.Collection;
import software.amazon.ion.ContainedValueException;
import software.amazon.ion.IonSexp;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.ValueVisitor;

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
     * Constructs a sexp value <em>not</em> backed by binary.
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
    IonSexpLite clone(IonContext parentContext)
    {
        return new IonSexpLite(this, parentContext);
    }

    @Override
    public IonSexpLite clone()
    {
        return clone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    public int hashCode(SymbolTableProvider symbolTableProvider) {
        return sequenceHashCode(HASH_SIGNATURE, symbolTableProvider);
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
