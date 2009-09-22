// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.impl.IonDatagramImpl;
import com.amazon.ion.impl.IonTextReaderImpl;
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
        IonTextReaderImpl textReader = system().newSystemReader(myText);
        IonDatagram datagram = new IonDatagramImpl(system(), catalog(), textReader);
        // Force symtab preparation  FIXME should not be necessary
        datagram.byteSize();

        return datagram.iterator();
    }

    @Override
    protected Iterator<IonValue> systemIterate()
        throws Exception
    {
        IonTextReaderImpl textReader = system().newSystemReader(myText);
        IonDatagram datagram = new IonDatagramImpl(system(), catalog(), textReader);
        // Force symtab preparation  FIXME should not be necessary
        datagram.byteSize();

        return datagram.systemIterator();
    }
}
