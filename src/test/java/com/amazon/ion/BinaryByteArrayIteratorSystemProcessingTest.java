// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion;

import org.junit.Test;

import java.util.Iterator;

import static com.amazon.ion.TestUtils.cleanCommentedHexBytes;
import static com.amazon.ion.TestUtils.hexStringToByteArray;


public class BinaryByteArrayIteratorSystemProcessingTest
    extends IteratorSystemProcessingTestCase
{
    private byte[] myBytes;

    @Override
    protected int expectedLocalNullSlotSymbolId()
    {
        return 0;
    }

    @Override
    protected void prepare(String text)
        throws Exception
    {
        myMissingSymbolTokensHaveText = false;
        myBytes = encode(text);
    }

    @Override
    protected Iterator<IonValue> iterate()
    {
        return system().iterate(myBytes);
    }

    @Override
    protected Iterator<IonValue> systemIterate()
    {
        return system().systemIterate(system().newSystemReader(myBytes));
    }

    private void prepareBinary(String commentedHexBytes) {
        myMissingSymbolTokensHaveText = false;
        myBytes = hexStringToByteArray(cleanCommentedHexBytes(commentedHexBytes));
    }

    @Test
    public void inlineFieldName() {
        prepareBinary(
            "E0 01 01 EA | Ion 1.1 IVM \n" +
            "FD          | Variable-length struct \n" +
            "0F          | Length 7 \n" +
            "01          | Switch to FlexSym field names \n" +
            "FF          | Inline field name, length 1 \n" +
            "61          | UTF-8 byte 'a' \n" +
            "D3          | Struct length 3 \n" +
            "09          | Field name SID 4 ('name') \n" +
            "A1          | Inline symbol value, length 1 \n" +
            "62          | UTF-8 byte 'b' \n"
        );
        Iterator<IonValue> iterator = systemIterate();
        assertTrue(iterator.hasNext());
        IonValue shouldBeAnIvm = iterator.next();
        assertEquals(IonType.SYMBOL, shouldBeAnIvm.getType());
        assertEquals("$ion_1_1", ((IonSymbol) shouldBeAnIvm).stringValue());
        assertTrue(iterator.hasNext());
        IonStruct struct = (IonStruct) iterator.next();
        assertEquals(1, struct.size());
        IonStruct nested = (IonStruct) struct.get("a");
        assertEquals("a", nested.getFieldName());
        assertEquals(1, nested.size());
        IonSymbol b = (IonSymbol) nested.get("name");
        assertEquals("name", b.getFieldName());
        assertEquals("b", b.stringValue());
        SymbolToken bToken = b.symbolValue();
        assertEquals("b", bToken.getText());
        assertEquals(-1, bToken.getSid());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void inlineAnnotation() {
        prepareBinary(
            "E0 01 01 EA | Ion 1.1 IVM \n" +
            "E8          | Two annotation FlexSyms follow \n" +
            "09          | Annotation SID 4 ('name') \n" +
            "FF          | Inline field name, length 1 \n" +
            "61          | UTF-8 byte 'a' \n" +
            "6F          | boolean false\n"
        );
        Iterator<IonValue> iterator = systemIterate();
        assertTrue(iterator.hasNext());
        IonValue shouldBeAnIvm = iterator.next();
        assertEquals(IonType.SYMBOL, shouldBeAnIvm.getType());
        assertEquals("$ion_1_1", ((IonSymbol) shouldBeAnIvm).stringValue());
        assertTrue(iterator.hasNext());
        IonBool value = (IonBool) iterator.next();
        String[] annotations = value.getTypeAnnotations();
        assertEquals(2, annotations.length);
        assertEquals("name", annotations[0]);
        assertEquals("a", annotations[1]);
        SymbolToken[] annotationTokens = value.getTypeAnnotationSymbols();
        assertEquals(2, annotationTokens.length);
        assertEquals("name", annotationTokens[0].getText());
        assertEquals(4, annotationTokens[0].getSid());
        assertEquals("a", annotationTokens[1].getText());
        assertEquals(-1, annotationTokens[1].getSid());
        assertFalse(value.booleanValue());
        assertFalse(iterator.hasNext());
    }
}
