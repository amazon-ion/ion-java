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

package com.amazon.ion.impl;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import org.junit.Ignore;
import org.junit.Test;


public class ValueWriterTest
    extends IonWriterTestCase
{
    private IonWriter myWriter;
    private IonDatagram myDatagram;
    private boolean myOutputInList;


    @Override
    protected IonWriter makeWriter(SymbolTable... imports)
        throws Exception
    {
        myOutputForm = OutputForm.DOM;

        myDatagram = system().newDatagram(imports);

        IonContainer c =
            (myOutputInList ? myDatagram.add().newEmptyList() : myDatagram);

        myWriter = system().newWriter(c);
        return myWriter;
    }

    @Override
    protected byte[] outputByteArray()
        throws Exception
    {
        return myDatagram.getBytes();
    }

    @Override
    protected void checkClosed()
    {
        // No-op.
    }

    @Override
    protected void checkFlushed(boolean expectFlushed)
    {
        // No-op.
    }

    //=========================================================================

    @Override
    public void testBadString(String text)
        throws Exception
    {
        // Tree writer accepts bad text, but it's caught at output time.
        iw = makeWriter();
        iw.writeString(text);
        iw.finish();

        try
        {
            myDatagram.toString();
            fail("expected exception");
        }
        catch (IllegalArgumentException e) { }
    }

    @Override
    public void testBadSymbol(String text)
        throws Exception
    {
        // Tree writer accepts bad text, but it's caught at output time.
        iw = makeWriter();
        iw.writeSymbol(text);
        iw.finish();

        try
        {
            myDatagram.toString();
            fail("expected exception");
        }
        catch (IllegalArgumentException e) { }
    }

    // TODO more thorough testing that you can't output from DOM w/ bad text


    @Test
    public void testWriteValuesWithSymtabIntoContainer()
        throws Exception
    {
        myOutputInList = true;

        SymbolTable fredSymtab =
            Symtabs.register(Symtabs.FRED_NAME, 1, catalog());
        SymbolTable gingerSymtab =
            Symtabs.register(Symtabs.GINGER_NAME, 1, catalog());
        String gingerSym = gingerSymtab.findKnownSymbol(1);

        // First setup some data to be copied.
        IonDatagram dg = system().newDatagram(gingerSymtab);
        dg.add().newSymbol(gingerSym);
        IonReader r = system().newReader(dg.getBytes());

        // Now copy that data into a non-top-level context
        iw = makeWriter(fredSymtab);
        iw.writeValues(r);

        IonDatagram result = reload();
        IonList l = (IonList) result.get(0);
        assertEquals(1, l.size());

        IonSymbol s = (IonSymbol) l.get(0);
        // Should've assigned a new SID
        checkSymbol(gingerSym, systemMaxId() + Symtabs.FRED_MAX_IDS[1] + 1, s);
    }


    @Override @Test @Ignore // TODO amazon-ion/ion-java/issues/8
    public void testWriteIVMImplicitly()
        throws Exception
    {
        super.testWriteIVMImplicitly();

        // TODO amazon-ion/ion-java/issues/20
        // assertEquals(2, myDatagram.size());
    }


    @Override @Test
    public void testWriteIVMExplicitly()
        throws Exception
    {
        super.testWriteIVMExplicitly();
        // TODO amazon-ion/ion-java/issues/20
        //assertEquals(2, myDatagram.size());
    }
}
