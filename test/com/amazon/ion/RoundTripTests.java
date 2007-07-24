/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

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

        public void setUp()
            throws Exception
        {
            super.setUp();
            myPrinter = new Printer();
            myBuilder = new StringBuilder();
        }

        public void tearDown()
            throws Exception
        {
            myPrinter = null;
            myBuilder = null;
            super.tearDown();
        }


        private String render(IonDatagram datagram)
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


        private byte[] encode(IonDatagram datagram)
        {
            return datagram.toBytes();
        }



        public void runTest()
        throws Exception
        {
            IonDatagram values = readIonText(myTestFile);

            // Turn the DOM back into text...
            String text1   = render(values);
            byte[] binary1 = encode(values);
            checkBinaryHeader(binary1);

            // Reload the first-trip text
            IonLoader loader = system().newLoader();
            try
            {
                values = loader.load(text1);
            }
            catch (Exception e)
            {
                String message =
                    "Error parsing file " + myTestFile.getName() +
                    " reprinted as:\n" + text1;
                throw new IonException(message, e);
            }

            String text2FromText   = render(values);
            byte[] binary2FromText = encode(values);
            checkBinaryHeader(binary2FromText);


            // Reload the first-trip binary

            values = loader.load(binary1);

            String text2FromBinary   = render(values);
            byte[] binary2FromBinary = encode(values);
            checkBinaryHeader(binary2FromBinary);
            assertEquals(binary1.length, binary2FromBinary.length);

            assertEquals("rendering from text vs from binary,",
                         text2FromText, text2FromBinary);

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



    public static TestSuite suite()
    {
        return new RoundTripTests();
    }


    public RoundTripTests()
    {
        super("good");
    }


    @Override
    protected Test makeTest(File ionFile)
    {
        return new RoundTripTest(ionFile);
    }
}
