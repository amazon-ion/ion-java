/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.ion;

import java.util.Iterator;


abstract class IteratorSystemProcessingTestCase
    extends SystemProcessingTestCase
{
    private Iterator<IonValue> myIterator;
    private IonValue myCurrentValue;


    abstract Iterator<IonValue> iterate()
        throws Exception;

    abstract Iterator<IonValue> systemIterate()
        throws Exception;


    @Override
    protected void startIteration() throws Exception
    {
        myIterator = iterate();
    }

    @Override
    protected void startSystemIteration() throws Exception
    {
        myIterator = systemIterate();
    }

    @Override
    protected void nextValue() throws Exception
    {
        myCurrentValue = myIterator.next();
    }

    @Override
    protected void stepIn() throws Exception
    {
        myIterator = ((IonContainer)myCurrentValue).iterator();
    }

    @Override
    protected void stepOut() throws Exception
    {

    }

    @Override
    protected IonType currentValueType() throws Exception
    {
        if (myCurrentValue == null) {
            return null;
        }
        return myCurrentValue.getType();
    }


    @Override
    Checker check()
    {
        return new IonValueChecker(myCurrentValue);
    }


    @Override
    protected void checkType(IonType expected)
    {
        assertSame(expected, myCurrentValue.getType());
    }

    @Override
    protected void checkInt(long expected) throws Exception
    {
        checkInt(expected, myCurrentValue);
    }

    @Override
    protected void checkDecimal(double expected) throws Exception
    {
        checkDecimal(expected, myCurrentValue);
    }

    @Override
    protected void checkFloat(double expected) throws Exception
    {
        checkFloat(expected, myCurrentValue);
    }

    @Override
    protected void checkString(String expected) throws Exception
    {
        checkString(expected, myCurrentValue);
    }

    @Override
    protected void checkSymbol(String expected) throws Exception
    {
        checkSymbol(expected, myCurrentValue);
    }

    @Override
    protected void checkSymbol(String expected, int expectedSid)
        throws Exception
    {
        checkSymbol(expected, expectedSid, myCurrentValue);
    }


    @Override
    protected void checkTimestamp(Timestamp expected) throws Exception
    {
        checkTimestamp(expected, myCurrentValue);
    }

    @Override
    SymbolTable currentSymtab()
    {
        return myCurrentValue.getSymbolTable();
    }

    @Override
    protected void checkEof() throws Exception
    {
        if (myIterator.hasNext())
        {
            fail("expected EOF, found " +  myIterator.next());
        }
    }

    @Override
    protected void checkString(String expectedValue,
                              String expectedRendering,
                              String ionData)
        throws Exception
    {
        super.checkString(expectedValue, expectedRendering, ionData);
        assertEquals(expectedRendering, myCurrentValue.toString());
    }
}
