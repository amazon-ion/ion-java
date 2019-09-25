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

package software.amazon.ion.util;

import software.amazon.ion.IonBlob;
import software.amazon.ion.IonBool;
import software.amazon.ion.IonClob;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonDecimal;
import software.amazon.ion.IonFloat;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonList;
import software.amazon.ion.IonNull;
import software.amazon.ion.IonSexp;
import software.amazon.ion.IonString;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonTimestamp;
import software.amazon.ion.IonValue;
import software.amazon.ion.ValueVisitor;

/**
 * A base class for extending Ion {@link ValueVisitor}s.
 * All <code>visit</code> methods are implemented to call
 * {@link #defaultVisit(IonValue)}.
 */
public abstract class AbstractValueVisitor
    implements ValueVisitor
{

    /**
     * Default visitation behavior, called by all <code>visit</code> methods
     * in {@link AbstractValueVisitor}.  Subclasses should override this unless
     * they override all <code>visit</code> methods.
     * <p>
     * This implementation always throws {@link UnsupportedOperationException}.
     *
     * @param value the value to visit.
     * @throws UnsupportedOperationException always thrown unless subclass
     * overrides this implementation.
     * @throws Exception subclasses can throw this; it will be propagated by
     * the other <code>visit</code> methods.
     */
    protected void defaultVisit(IonValue value) throws Exception
    {
        throw new UnsupportedOperationException();
    }

    public void visit(IonBlob value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonBool value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonClob value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonDatagram value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonDecimal value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonFloat value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonInt value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonList value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonNull value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonSexp value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonString value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonStruct value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonSymbol value) throws Exception
    {
        defaultVisit(value);
    }

    public void visit(IonTimestamp value) throws Exception
    {
        defaultVisit(value);
    }
}
