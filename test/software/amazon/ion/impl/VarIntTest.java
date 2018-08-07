package software.amazon.ion.impl;

import org.junit.Test;
import software.amazon.ion.IonException;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.system.SimpleCatalog;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;

public class VarIntTest extends IonTestCase {

    @Test
    public void readMaxVarUInt() throws Exception {
        assertEquals(Integer.MAX_VALUE, makeReader("077F7F7FFF").readVarUInt());
    }

    @Test(expected = IonException.class)
    public void overflowVarUInt() throws Exception {
        makeReader("0800000080").readVarUInt(); // Integer.MAX_VALUE + 1
    }

    @Test(expected = IonException.class)
    public void readEOFVarUInt() throws Exception {
        makeReader("").readVarUInt();
    }

    @Test
    public void readMaxVarUIntOrEOF() throws Exception {
        assertEquals(Integer.MAX_VALUE, makeReader("077F7F7FFF").readVarUIntOrEOF());
    }

    @Test(expected = IonException.class)
    public void overflowVarUIntOrEOF() throws Exception {
        makeReader("0800000080").readVarUIntOrEOF(); // Integer.MAX_VALUE + 1
    }

    @Test
    public void readEOFVarUIntOrEOF() throws Exception {
        assertEquals(UnifiedInputStreamX.EOF, makeReader("").readVarUIntOrEOF());
    }

    @Test
    public void readMaxVarInt() throws Exception {
        assertEquals(Integer.MAX_VALUE, makeReader("077F7F7FFF").readVarInt());
    }

    @Test
    public void readMinVarInt() throws Exception {
        assertEquals(Integer.MIN_VALUE, makeReader("4800000080").readVarInt());
    }

    @Test(expected = IonException.class)
    public void readVarIntOverflow() throws Exception {
        makeReader("0800000080").readVarInt(); // Integer.MAX_VALUE + 1
    }

    @Test(expected = IonException.class)
    public void readVarIntUnderflow() throws Exception {
        makeReader("4800000081").readVarInt(); // Integer.MIN_VALUE - 1
    }

    @Test
    public void readMaxVarInteger() throws Exception {
        assertEquals(Integer.MAX_VALUE, (int) makeReader("077F7F7FFF").readVarInteger());
    }

    @Test
    public void readMinVarInteger() throws Exception {
        assertEquals(Integer.MIN_VALUE, (int) makeReader("4800000080").readVarInteger());
    }

    @Test(expected = IonException.class)
    public void readVarIntegerOverflow() throws Exception {
        makeReader("0800000080").readVarInteger(); // Integer.MAX_VALUE + 1
    }

    @Test(expected = IonException.class)
    public void readVarIntegerUnderflow() throws Exception {
        makeReader("4800000081").readVarInt(); // Integer.MIN_VALUE - 1
    }

    @Test
    public void readVarIntegerNegativeZero() throws Exception {
        assertNull(makeReader("C0").readVarInteger());
    }

    private IonReaderBinaryUserX makeReader(String hex) throws Exception {
        ByteArrayInputStream input = new ByteArrayInputStream(DatatypeConverter.parseHexBinary("E00100EA" + hex));

        UnifiedInputStreamX uis = UnifiedInputStreamX.makeStream(input);
        uis.skip(4);

        return new IonReaderBinaryUserX(new SimpleCatalog(), LocalSymbolTable.DEFAULT_LST_FACTORY, uis, 0);
    }
}