// Copyright (c) 2007-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.junit.IonAssert;
import java.io.File;
import org.junit.Test;

public class EquivsTest
    extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        TestUtils.testdataFiles(TestUtils.GLOBAL_SKIP_LIST, "equivs");

    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }


    @Test
    public void testEquivsOverFile()
    throws Exception
    {
        IonDatagram dg = loader().load(myTestFile);
        runEquivs(dg);
    }

    @Test
    public void testEquivsOverString()
    throws Exception
    {
        String ionText = _Private_Utils.utf8FileToString(myTestFile);
        IonDatagram dg = loader().load(ionText);
        runEquivs(dg);
    }

    private void runEquivs(final IonDatagram sequences) {
        int sequenceCount = sequences.size();

        assertTrue("File must have at least one sequence",
                   sequenceCount > 0);

        for (int i = 0; i < sequenceCount; i++)
        {
            IonSequence sequence = (IonSequence) sequences.get(i);

            int valueCount = sequence.size();

            assertTrue("Each sequence must have at least two values",
                       valueCount > 1);

            for (int j = 0; j < valueCount - 1; j++)
            {
                IonValue current = sequence.get(j);
                IonValue next    = sequence.get(j + 1);

                try
                {
                    checkEquivalence(current, next);
                }
                catch (Throwable e)
                {
                    String message =
                        "Error comparing children " + j + " and " + (j+1) +
                        " of equivalence sequence " + i + " (zero-based)";
                    Error wrap = new AssertionError(message);
                    wrap.initCause(e);
                    throw wrap;
                }
            }
        }
    }

    protected void checkEquivalence(IonValue left, IonValue right)
    {
        // the extra if allows sitting a break point on failures
        if (left.equals(right) == false) {
            IonAssert.assertIonEquals(left, right);
        }
        assertEquals("Equal values have unequal hashes",
                     left.hashCode(), right.hashCode());
    }
}
