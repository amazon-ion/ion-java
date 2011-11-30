// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.junit.Injected.Inject;


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

    private LoadTime myLoadTime;
    private String myText;
    private IonDatagram myDatagram;


    public void setLoadTime(LoadTime time)
    {
        myLoadTime = time;
    }

    private void load(LoadTime now)
    {
        if (myLoadTime == now)
        {
            myDatagram = loader().load(myText);
        }
    }

    // ========================================================================

    @Override
    protected void prepare(String text)
    {
        myText = text;
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
    protected boolean checkMissingSymbol(String expected,
                                         int expectedSymbolTableSid,
                                         int expectedLocalSid)
        throws Exception
    {
        if (myLoadTime == LoadTime.LOAD_IN_PREPARE)
        {
            // The datagram loaded the text and encoded sid during prepare,
            // so the DOM retains the original sid.
            checkSymbol(expected, expectedSymbolTableSid);
            return false;
        }

        // The datagram was loaded after the catalog changed.
        // We have no way to find the encoded sid, so a local one is assigned.
        checkSymbol(expected, expectedLocalSid);
        return true;
    }
}
