/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import com.amazon.ion.impl.IonDatagramImpl;
import com.amazon.ion.impl.IonTextReader;
import java.util.Iterator;

/**
 *
 */
public class NewDatagramIteratorSystemProcessingTest
    extends IteratorSystemProcessingTest
{
    @Override
    protected Iterator<IonValue> iterate(String text)
        throws Exception
    {
        IonTextReader textReader = system().newSystemReader(text);

        IonDatagram datagram = new IonDatagramImpl(system(), textReader);

        // Force symtab preparation  FIXME should not be necessary
        datagram.byteSize();

        return datagram.iterator();
    }

    @Override
    protected Iterator<IonValue> systemIterate(String text)
        throws Exception
    {
//        IonLoader loader = loader();
//        IonDatagram datagram = loader.load(text);
        IonTextReader textReader = system().newSystemReader(text);

      IonDatagram datagram = new IonDatagramImpl(system(), textReader);
        return datagram.systemIterator();
    }
}
