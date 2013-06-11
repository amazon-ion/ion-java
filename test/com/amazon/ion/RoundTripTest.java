// Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.GOOD_IONTESTS_FILES;
import static com.amazon.ion.TestUtils.TEXT_ONLY_FILTER;
import static com.amazon.ion.TestUtils.testdataFiles;
import static com.amazon.ion.junit.IonAssert.assertIonEquals;

import com.amazon.ion.TestUtils.And;
import com.amazon.ion.impl.IonWriterUserBinary;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.streaming.ReaderCompare;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.util.Printer;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Processes all text files in the "good" suite, transforming between text and
 * binary twice to ensure that the process is equivalent.
 */
public class RoundTripTest
    extends IonTestCase
{
    private Printer       myPrinter;
    private StringBuilder myBuilder;

    //------------------------------------------------------------------------

    /**
     * We don't use binary files since some of those have non-canonical
     * encodings that won't round-trip.
     */
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(new And(TEXT_ONLY_FILTER,
                              GLOBAL_SKIP_LIST),
                      GOOD_IONTESTS_FILES);

    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }

    //------------------------------------------------------------------------

    // Using an enum makes the test names more understandable than a boolean.
    private enum CopySpeed { slow, fast }

    @Inject("copySpeed")
    public static final CopySpeed[] COPY_SPEEDS = CopySpeed.values();

    public void setCopySpeed(CopySpeed speed)
    {
        IonWriterUserBinary.ourFastCopyEnabled = (speed == CopySpeed.fast);
    }

    @After
    public void resetFastCopyFlag()
    {
        IonWriterUserBinary.ourFastCopyEnabled =
            IonWriterUserBinary.OUR_FAST_COPY_DEFAULT;
    }

    //------------------------------------------------------------------------

    @Override
    @Before
    public void setUp()
    throws Exception
    {
        super.setUp();
        myPrinter = new Printer();
        myBuilder = new StringBuilder();
    }

    @Override
    @After
    public void tearDown()
    throws Exception
    {
        myPrinter = null;
        myBuilder = null;
        super.tearDown();
    }


    private String renderSystemView(IonDatagram datagram)
    throws IOException
    {
        // TODO use Printer in raw mode.
        for (Iterator i = datagram.systemIterator(); i.hasNext();)
        {
            IonValue value = (IonValue) i.next();
            myPrinter.print(value, myBuilder);
            myBuilder.append('\n');
        }

        String text = myBuilder.toString();
        myBuilder.setLength(0);
        return text;
    }

    private String renderUserView(IonDatagram datagram)
    throws IOException
    {
        // TODO use Printer in raw mode.
        for (Iterator i = datagram.iterator(); i.hasNext();)
        {
            IonValue value = (IonValue) i.next();
            myPrinter.print(value, myBuilder);
            myBuilder.append('\n');
        }

        String text = myBuilder.toString();
        myBuilder.setLength(0);
        return text;
    }

    private byte[] encode(IonDatagram datagram)
    {
        return datagram.getBytes();
    }



    @Test
    public void test()
    throws Exception
    {
        IonDatagram values = loader().load(myTestFile);

        // Turn the DOM back into text...
        String text1   = renderSystemView(values);
        byte[] binary1 = encode(values);
        checkBinaryHeader(binary1);

        // Reload the first-trip text
        IonLoader loader = system().newLoader();
        IonDatagram dgFromText;
        try
        {
            dgFromText = loader.load(text1);
        }
        catch (Exception e)
        {
            String message =
                "Error parsing file " + myTestFile.getName() +
                " reprinted as:\n" + text1;
            throw new IonException(message, e);
        }

        String text2FromText   = renderUserView(dgFromText);
        byte[] binary2FromText = encode(dgFromText);
        checkBinaryHeader(binary2FromText);


        // Reload the first-trip binary

        IonDatagram dgFromBinary = loader.load(binary1);

        String text2FromBinary   = renderUserView(dgFromBinary);
        byte[] binary2FromBinary = encode(dgFromBinary);
        checkBinaryHeader(binary2FromBinary);

        // check strict data equivalence
        IonDatagram dgbinary2FromBinary = loader.load(binary2FromBinary);
        assertIonEquals(dgFromBinary, dgbinary2FromBinary);

        if (!compareRenderedTextImages(text2FromText, text2FromBinary))
        {
            fail("different printing from text vs from binary");
        }

        // check strict data equivalence
        IonDatagram dgBinary2FromText = loader.load(binary2FromText);
        assertIonEquals(dgFromBinary, dgBinary2FromText);
    }


    private final static String ivm = "$ion_1_0";
    static boolean compareRenderedTextImages(String s1, String s2) {
        assert (s1 != null);
        assert (s2 != null);

        if (s1 == s2) return true;
        if (s1.equals(s2)) return true;

        if (!s1.startsWith(ivm)) {
            s1 = ivm + " " + s1;
        }
        if (!s2.startsWith(ivm)) {
            s2 = ivm + " " + s2;
        }
        // TODO the next step is, if they are still not the same, then
        //      convert them to binary and back to string and compare again
        return s1.equals(s2);
    }


    private void checkTextOutput(IonTextWriterBuilder builder)
        throws Exception
    {
        byte[] outBytes;

        FileInputStream fileIn = new FileInputStream(myTestFile);
        try
        {
            IonReader r0 = system().newSystemReader(fileIn);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IonWriter w = builder.build(out);
            w.writeValues(r0);
            w.close();

            outBytes = out.toByteArray();
        }
        finally
        {
            fileIn.close();
        }

        fileIn = new FileInputStream(myTestFile);
        try
        {
            IonReader r0 = system().newSystemReader(fileIn);
            IonReader r1 = system().newSystemReader(outBytes);

            ReaderCompare.compare(r0, r1);
        }
        finally
        {
            fileIn.close();
        }
    }


    @Test
    public void testStandardTextWriter()
        throws Exception
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        checkTextOutput(b);
    }


    @Test
    public void testTextWriterLongStrings()
        throws Exception
    {
        IonTextWriterBuilder b =
            IonTextWriterBuilder.standard()
            .withLongStringThreshold(1);
        checkTextOutput(b);
    }
}
