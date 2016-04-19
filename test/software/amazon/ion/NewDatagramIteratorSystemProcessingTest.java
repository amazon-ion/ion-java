/*
 * Copyright 2008-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion;

import java.util.Iterator;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;

/**
 * TODO replicates other tests in this hierarchy
 */
public class NewDatagramIteratorSystemProcessingTest
    extends IteratorSystemProcessingTestCase
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
