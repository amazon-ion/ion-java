/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package software.amazon.ion;

import java.util.Iterator;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonLoader;
import software.amazon.ion.IonValue;

public class DatagramIteratorSystemProcessingTest
    extends IteratorSystemProcessingTestCase
{
    private String myText;

    @Override
    protected void prepare(String text)
        throws Exception
    {
        myText = text;
    }

    protected IonDatagram load()
        throws Exception
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(myText);
        return datagram;
    }


    @Override
    protected Iterator<IonValue> iterate()
        throws Exception
    {
        IonDatagram datagram = load();
        return datagram.iterator();
    }

    @Override
    protected Iterator<IonValue> systemIterate()
        throws Exception
    {
        IonDatagram datagram = load();
        return datagram.systemIterator();
    }
}
