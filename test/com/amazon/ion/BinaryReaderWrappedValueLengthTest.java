package com.amazon.ion;

import static com.amazon.ion.BitUtils.bytes;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


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
