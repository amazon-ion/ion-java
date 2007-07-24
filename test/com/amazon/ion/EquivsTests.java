/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.File;
import junit.framework.TestSuite;

public class EquivsTests
    extends DirectoryTestSuite
{
    private static class EquivsTest
        extends FileTestCase
    {
        public EquivsTest(File ionText)
        {
            super(ionText);
        }

        public void runTest()
            throws Exception
        {
            IonDatagram values = readIonText(myTestFile);
            int valueCount = values.size();

            assertTrue("File must have at least two values",
                       valueCount > 1);

            for (int i = 0; i < valueCount - 1; i++)
            {
                IonValue current = values.get(i);
                IonValue next    = values.get(i + 1);

                assertIonEquals(current, next);
            }
        }
    }


    public static TestSuite suite()
    {
        return new EquivsTests();
    }


    public EquivsTests()
    {
        super("equivs");
    }


    @Override
    protected EquivsTest makeTest(File ionFile)
    {
        return new EquivsTest(ionFile);
    }
}
