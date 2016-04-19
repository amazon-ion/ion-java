/*
 * Copyright 2016-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.junit.Test;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonType;

public class IonReaderToIonValueTest extends IonTestCase {
    @Test
    public void testIonValueNoFieldNameFromReader() {
        final IonReader reader = system().newReader("{a:{b:5}}");
        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertEquals(IonType.STRUCT, reader.next());

        final String fieldName = reader.getFieldName();
        assertEquals("a", fieldName);

        final IonStruct struct = (IonStruct) system().newValue(reader);
        assertNull(struct.getFieldName());

        final IonInt child = (IonInt) struct.get("b");
        assertEquals(5, child.longValue());
    }
}
