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

import static software.amazon.ion.DatagramMaker.FROM_BYTES_TEXT;

import org.junit.After;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonReader;
import software.amazon.ion.junit.Injected.Inject;


public class DatagramTreeReaderSystemProcessingTest
    extends ReaderSystemProcessingTestCase
{
    @Inject("datagramMaker")
    public static final DatagramMaker[] DATAGRAM_MAKERS =
        DatagramMaker.valuesExcluding(FROM_BYTES_TEXT    // TODO UTF8 issues
                                      );

    private DatagramMaker myDatagramMaker;
    private String myText;
    private byte[] myBinary;
    private IonDatagram myDatagram;

    public void setDatagramMaker(DatagramMaker maker)
    {
        myDatagramMaker = maker;
    }

    /**
     * Load the datagram. The datagram is only loaded during
     * {@link #read()} and {@link #systemRead()}.
     */
    private void load()
    {
        if (myDatagramMaker.sourceIsBinary())
        {
            myDatagram = myDatagramMaker.newDatagram(system(), myBinary);
        }
        else
        {
            myDatagram = myDatagramMaker.newDatagram(system(), myText);
        }
    }

    @After @Override
    public void tearDown() throws Exception
    {
        myText = null;
        myDatagram = null;
        super.tearDown();
    }

    // ========================================================================

    @Override
    protected void prepare(String text)
    {
        myMissingSymbolTokensHaveText = myDatagramMaker.sourceIsText();

        myText = text;
        if (myDatagramMaker.sourceIsBinary())
        {
            IonDatagram dg = loader().load(text);
            myBinary = dg.getBytes();
        }
    }

    @Override
    public IonReader read() throws Exception
    {
        load();
        return system().newReader(myDatagram);
    }

    @Override
    public IonReader systemRead() throws Exception
    {
        load();
        return system().newSystemReader(myDatagram);
    }

}
