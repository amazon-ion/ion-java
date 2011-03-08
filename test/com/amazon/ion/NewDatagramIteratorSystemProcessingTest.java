// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.impl.IonValuePrivate;
import java.util.Iterator;

/**
 * TODO replicates other tests in this hierarchy
 */
public class NewDatagramIteratorSystemProcessingTest
    extends IteratorSystemProcessingTest
{
    private String myText;

    @Override
    protected void prepare(String text)
        throws Exception
    {
        myText = text;
    }

    @Override
    protected Iterator<IonValue> iterate()
        throws Exception
    {
        IonReader reader = system().newSystemReader(myText);

        // was: new IonDatagramImpl(system(), catalog(), reader);
        // Force symtab preparation  FIXME should not be necessary
        // datagram.byteSize();
        IonDatagram datagram = system().newDatagram();

        // not newTreeUserWriter since we don't want to see local
        // symbol tables even if they're required
        IonWriter writer = system().newTreeWriter(datagram);

        writer.writeValues(reader);
        writer.close();

        // not needed, used populateSymbolValue instead: datagram.deepMaterialize();
        ((IonValuePrivate)datagram).populateSymbolValues(null);

        return datagram.iterator();
    }

    @Override
    protected Iterator<IonValue> systemIterate()
        throws Exception
    {
        IonReader reader = system().newSystemReader(myText);
        // was: IonDatagram datagram =
        //         new IonDatagramImpl(system(), catalog(), reader);
        //      // Force symtab preparation  FIXME should not be necessary
        //      datagram.byteSize();

        IonDatagram datagram = system().newDatagram();

        // FIXME: hack - maybe this is simply surpassed by the lite
        //               but here we can have an extra $ion_1_0 if
        //               it is present in the input text since the
        //               newDatagram() forces one into the binary
        //               buffer as well.


        IonWriter writer = system().newTreeSystemWriter(datagram);
        writer.writeValues(reader);
        writer.close();

        return datagram.systemIterator();
    }
}
