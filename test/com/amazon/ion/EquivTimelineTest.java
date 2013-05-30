// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.TestUtils.testdataFiles;

import com.amazon.ion.junit.Injected.Inject;
import java.io.File;



public class EquivTimelineTest
    extends EquivsTest
{
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(TestUtils.GLOBAL_SKIP_LIST,
                      "good/timestamp/equivTimeline");


    @Override
    protected void checkEquivalence(IonValue left, IonValue right,
                                    boolean expectedEquality)
    {
        // expectedEquality isn't used in this method
        IonTimestamp lDom = (IonTimestamp) left;
        IonTimestamp rDom = (IonTimestamp) right;

        Timestamp lTime = lDom.timestampValue();
        Timestamp rTime = rDom.timestampValue();

        assertEquals("Timestamp.compareTo", 0, lTime.compareTo(rTime));
        assertEquals("Timestamp.compareTo", 0, rTime.compareTo(lTime));

        assertEquals("millis", lTime.getMillis(), rTime.getMillis());
        assertEquals("calendar", lTime.calendarValue(), rTime.calendarValue());
        assertEquals("date", lTime.dateValue(), rTime.dateValue());
    }
}
