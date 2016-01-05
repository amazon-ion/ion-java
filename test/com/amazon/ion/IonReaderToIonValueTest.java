package com.amazon.ion;

import org.junit.Test;

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
