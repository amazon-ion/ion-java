// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.util.Printer;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Processes all files in the "good" suite, transforming between text and
 * binary twice to ensure that the process is equivalent.
 */
public class RoundTripTests
    extends DirectoryTestSuite
{
    private static class RoundTripTest
    extends FileTestCase
    {
        private Printer       myPrinter;
        private StringBuilder myBuilder;


        public RoundTripTest(File ionText)
        {
            super(ionText);
        }

        @Override
        public void setUp()
            throws Exception
        {
            super.setUp();
            myPrinter = new Printer();
            myBuilder = new StringBuilder();
        }

        @Override
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



        @Override
        public void runTest()
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
            assertEquals(binary1.length, binary2FromBinary.length);

            if (!compareRenderedTextImages(text2FromText, text2FromBinary))
            {
                fail("different printing from text vs from binary");
            }

            assertEquals("encoded size from text vs from binary",
                         binary2FromText.length,
                         binary2FromBinary.length);

            for (int i = 0; i < binary2FromText.length; i++)
            {
                if (binary2FromText[i] != binary2FromBinary[i])
                {
                    fail("Binary data differs at index " + i);
                }
            }
        }
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


    public static TestSuite suite()
    {
        return new RoundTripTests();
    }


    public RoundTripTests()
    {
        super("good", "equivs");
    }


    @Override
    protected Test makeTest(File ionFile)
    {
        String fileName = ionFile.getName();
        if (fileName.endsWith(".ion"))
        {
            return new RoundTripTest(ionFile);
        }
        return null;
    }

    @Override
    protected String[] getFilesToSkip()
    {
        return new String[]
        {
             // "equivs/symbols.ion",
        };
    }
}
