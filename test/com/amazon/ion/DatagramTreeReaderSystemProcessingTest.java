// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.DatagramMaker.FROM_BYTES_TEXT;

import com.amazon.ion.junit.Injected.Inject;
import org.junit.After;


/**
 *
 */
public class DatagramTreeReaderSystemProcessingTest
    extends ReaderSystemProcessingTestCase
{
    /** When is the datagram loaded? */
    protected enum LoadTime { LOAD_IN_PREPARE, LOAD_IN_READ }

    @Inject("loadTime")
    public static final LoadTime[] LOAD_TIMES = LoadTime.values();

    @Inject("datagramMaker")
    public static final DatagramMaker[] DATAGRAM_MAKERS =
        DatagramMaker.valuesExcluding(FROM_BYTES_TEXT    // TODO UTF8 issues
                                      );

    private LoadTime myLoadTime;
    private DatagramMaker myDatagramMaker;
    private String myText;
    private byte[] myBinary;
    private IonDatagram myDatagram;


    public void setLoadTime(LoadTime time)
    {
        myLoadTime = time;
    }

    public void setDatagramMaker(DatagramMaker maker)
    {
        myDatagramMaker = maker;
    }


    private void load(LoadTime now)
    {
        if (myLoadTime == now)
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
        myText = text;
        if (myDatagramMaker.sourceIsBinary())
        {
            IonDatagram dg = loader().load(text);
            myBinary = dg.getBytes();
        }

        load(LoadTime.LOAD_IN_PREPARE);
    }

    @Override
    public IonReader read() throws Exception
    {
        load(LoadTime.LOAD_IN_READ);
        return system().newReader(myDatagram);
    }

    @Override
    public IonReader systemRead() throws Exception
    {
        load(LoadTime.LOAD_IN_READ);
        return system().newSystemReader(myDatagram);
    }

    @Override
    boolean checkMissingFieldName(String expectedText,
                                  int expectedEncodedSid,
                                  int expectedLocalSid)
        throws Exception
    {
        if (myLoadTime == LoadTime.LOAD_IN_PREPARE)
        {
            // The datagram loaded the text and encoded sid during prepare,
            // so the DOM retains the original sid.
            checkFieldName(expectedText, expectedEncodedSid);
            return false;
        }

        if (myDatagramMaker.sourceIsBinary())
        {
            // TODO ION-58
            checkFieldName(null, expectedEncodedSid);
            return false;
        }

        // The datagram was loaded after the catalog changed.
        // We have no way to find the encoded sid, so a local one is assigned.
        checkFieldName(expectedText, expectedLocalSid);
        return true;
    }

    @Override
    protected boolean checkMissingSymbol(String expectedText,
                                         int expectedEncodedSid,
                                         int expectedLocalSid)
        throws Exception
    {
        if (myLoadTime == LoadTime.LOAD_IN_PREPARE)
        {
            // The datagram loaded the text and encoded sid during prepare,
            // so the DOM retains the original sid.
            checkSymbol(expectedText, expectedEncodedSid);
            return false;
        }

        if (myDatagramMaker.sourceIsBinary())
        {
            checkSymbol(null, expectedEncodedSid);
            return false;
        }

        // The datagram was loaded after the catalog changed.
        // We have no way to find the encoded sid, so a local one is assigned.
        checkSymbol(expectedText, expectedLocalSid);
        return true;
    }
}
