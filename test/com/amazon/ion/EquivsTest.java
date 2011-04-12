// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.junit.IonAssert;
import java.io.File;
import junit.framework.AssertionFailedError;
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
    public void test()
    throws Exception
    {
        runEquivs("Datagram Load", load(myTestFile));
        final IonDatagram dgFromString = loadAsJavaString(myTestFile);
        if (dgFromString != null) {
            runEquivs("Datagram Java String Load", dgFromString);
        }
    }

    private void runEquivs(final String type, final IonDatagram sequences) {
        int sequenceCount = sequences.size();

        assertTrue(type + ": File must have at least one sequence",
                   sequenceCount > 0);

        for (int i = 0; i < sequenceCount; i++)
        {
            IonSequence sequence = (IonSequence) sequences.get(i);

            int valueCount = sequence.size();

            assertTrue(type + ": Each sequence must have at least two values",
                       valueCount > 1);

            for (int j = 0; j < valueCount - 1; j++)
            {
                IonValue current = sequence.get(j);
                IonValue next    = sequence.get(j + 1);

                try
                {
                    // the extra if allows us (me) to set a break point on failures here, specifically here.
                    if (current.equals(next) == false) {
                        IonAssert.assertIonEquals(type, current, next);
                    }
                    assertEquals(type + ": Equal values have unequal hashes",
                                 current.hashCode(), next.hashCode());
                }
                catch (Throwable e)
                {
                    String message =
                        type +
                        ": Error comparing children " + j + " and " + (j+1) +
                        " of equivalence sequence " + i + " (zero-based)";
                    Error wrap = new AssertionFailedError(message);
                    wrap.initCause(e);
                    throw wrap;
                }
            }
        }
    }
}
