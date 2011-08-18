// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.impl.IonImplUtils;
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
        String ionText = IonImplUtils.utf8FileToString(myTestFile);
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
                    // the extra if allows us (me) to set a break point on failures here, specifically here.
                    if (current.equals(next) == false) {
                        IonAssert.assertIonEquals(current, next);
                    }
                    assertEquals("Equal values have unequal hashes",
                                 current.hashCode(), next.hashCode());
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
}
