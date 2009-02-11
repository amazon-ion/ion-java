// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

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

        @Override
        public void runTest()
            throws Exception
        {
            IonDatagram sequences = load(myTestFile);
            int sequenceCount = sequences.size();

            assertTrue("File must have at least one sequence",
                       sequenceCount > 0);

            for (int i = 0; i < sequenceCount - 1; i++)
            {
                IonSequence sequence = (IonSequence) sequences.get(i);

                int valueCount = sequence.size();

                assertTrue("Each sequence must have at least two values",
                           valueCount > 1);

                for (int j = 0; j < valueCount - 1; j++)
                {
                    IonValue current = sequence.get(j);
                    IonValue next    = sequence.get(j + 1);

                    assertIonEquals(current, next);
                }
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
