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
import software.amazon.ion.IonList;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.ValueVisitor;

final class IonListLite
    extends IonSequenceLite
    implements IonList
{
    private static final int HASH_SIGNATURE =
        IonType.LIST.toString().hashCode();


    /**
     * Constructs a null or empty list.
     *
     * @param makeNull indicates whether this should be <code>null.list</code>
     * (if <code>true</code>) or an empty sequence (if <code>false</code>).
     */
    IonListLite(ContainerlessContext context, boolean makeNull)
    {
        super(context, makeNull);
    }

    IonListLite(IonListLite existing, IonContext context)
    {
        super(existing, context);
    }

    /**
     * Constructs a list value <em>not</em> backed by binary.
     *
     * @param elements
     *   the initial set of child elements.  If <code>null</code>, then the new
     *   instance will have <code>{@link #isNullValue()} == true</code>.
     *
     * @throws ContainedValueException if any value in <code>elements</code>
     * has <code>{@link IonValue#getContainer()} != null</code>.
     */
    IonListLite(ContainerlessContext context,
                Collection<? extends IonValue> elements)
        throws ContainedValueException
    {
        super(context, elements);
    }

    @Override
    IonListLite clone(IonContext parentContext)
    {
        return new IonListLite(this, parentContext);
    }

    @Override
    public IonListLite clone()
    {
        return clone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    int hashCode(SymbolTableProvider symbolTableProvider) {
        return sequenceHashCode(HASH_SIGNATURE, symbolTableProvider);
    }

    @Override
    public IonType getType()
    {
        return IonType.LIST;
    }

    @Override
    public void accept(ValueVisitor visitor)
        throws Exception
    {
        visitor.visit(this);
    }
}
