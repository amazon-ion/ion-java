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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static software.amazon.ion.BitUtils.bytes;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;


public class BinaryReaderWrappedValueLengthTest
    extends IonTestCase
{
    private static final byte[] OVERRUN = bytes(
        // IVM
        0xE0,
        0x01,
        0x00,
        0xEA,

        // this should be 0xE4 (size 2 too long)
        0xE6,

        0x81,
        0x84,

        0x71,
        0x04,

        0x71,
        0x04
    );

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void readInto() throws Exception
    {
        thrown.expect(IonException.class);
        thrown.expectMessage("Wrapper length mismatch: wrapper 11 wrapped value 9 at position 8");

        final IonReader in = system().newReader(OVERRUN);

        assertEquals(IonType.SYMBOL, in.next());
        assertEquals(asList("name"), asList(in.getTypeAnnotations()));
        assertEquals("name", in.stringValue());

        assertEquals(IonType.SYMBOL, in.next());
        assertEquals(emptyList(), asList(in.getTypeAnnotations()));
        assertEquals("name", in.stringValue());
    }

    @Test
    public void readOver() throws Exception
    {
        thrown.expect(IonException.class);
        thrown.expectMessage("Wrapper length mismatch: wrapper 11 wrapped value 9 at position 8");

        final IonReader in = system().newReader(OVERRUN);

        assertEquals(IonType.SYMBOL, in.next());
        assertEquals(IonType.SYMBOL, in.next());
    }
}
