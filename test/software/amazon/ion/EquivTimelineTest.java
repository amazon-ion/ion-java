/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.ion;

import static software.amazon.ion.TestUtils.EQUIVS_TIMESTAMP_IONTESTS_FILES;
import static software.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static software.amazon.ion.TestUtils.testdataFiles;

import software.amazon.ion.junit.Injected.Inject;
import java.io.File;
import java.io.IOException;

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

    @Override
    public void roundTripEquivalence(IonDatagram input, boolean myExpectedEquality) throws IOException {
        IonDatagram[] data = roundTripDatagram(input);
        for(int i = 0; i < data.length; i++) {
            runEquivalenceChecks(data[i], myExpectedEquality);
            for(int j = i + 1; j < data.length; j++) {
                for(int seqIndice = 0; seqIndice < data[i].size(); seqIndice++) {
                    int maxTimeStampIndice = ((IonSequence)data[i].get(seqIndice)).size();
                    for(int timeStampIndice = 0; timeStampIndice < maxTimeStampIndice; timeStampIndice++) {
                        IonValue timeStamp1 = ((IonSequence)(data[i].get(seqIndice))).get(timeStampIndice);
                        IonValue timeStamp2 = ((IonSequence)(data[j].get(seqIndice))).get(timeStampIndice);
                        checkEquivalence(timeStamp1, timeStamp2, true);
                    }
                }
            }
        }
    }
}
