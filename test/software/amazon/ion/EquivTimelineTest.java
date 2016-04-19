/*
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion;

import static software.amazon.ion.TestUtils.EQUIVS_TIMESTAMP_IONTESTS_FILES;
import static software.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static software.amazon.ion.TestUtils.testdataFiles;

import java.io.File;
import software.amazon.ion.IonTimestamp;
import software.amazon.ion.IonValue;
import software.amazon.ion.Timestamp;
import software.amazon.ion.junit.Injected.Inject;

public class EquivTimelineTest
    extends EquivsTest
{
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(GLOBAL_SKIP_LIST,
                      EQUIVS_TIMESTAMP_IONTESTS_FILES);


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
