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
        int actual = makeReader("077F7F7FFF").readVarUInt();

        assertEquals(Integer.MAX_VALUE, actual);
    }

    @Test(expected = IonException.class)
    public void overflowVarUInt() throws Exception {
        makeReader("0800000080").readVarUInt(); // Integer.MAX_VALUE + 1
    }

    @Test
    public void readMaxVarInt() throws Exception {
        int actual = makeReader("077F7F7FFF").readVarInt();

        assertEquals(Integer.MAX_VALUE, actual);
    }

    @Test
    public void readMinVarInt() throws Exception {
        int actual = makeReader("4800000080").readVarInt();

        assertEquals(Integer.MIN_VALUE, actual);
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
        Integer actual = makeReader("077F7F7FFF").readVarInteger();

        assertEquals(Integer.MAX_VALUE, (int) actual);
    }

    @Test
    public void readMinVarInteger() throws Exception {
        Integer actual = makeReader("4800000080").readVarInteger();

        assertEquals(Integer.MIN_VALUE, (int) actual);
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
        Integer actual = makeReader("C0").readVarInteger();

        assertNull(actual);
    }

    private IonReaderBinaryUserX makeReader(String hex) throws Exception {

        ByteArrayInputStream input = new ByteArrayInputStream(DatatypeConverter.parseHexBinary("E00100EA" + hex));

        UnifiedInputStreamX uis = UnifiedInputStreamX.makeStream(input);
        uis.skip(4);

        return new IonReaderBinaryUserX(new SimpleCatalog(), LocalSymbolTable.DEFAULT_LST_FACTORY, uis, 0);
    }
}