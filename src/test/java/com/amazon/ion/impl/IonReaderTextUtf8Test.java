// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.system.IonReaderBuilder;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class IonReaderTextUtf8Test {

    /**
     * Builds Ion text from a mix of ASCII fragments and raw byte values, so that
     * byte sequences that are not valid UTF-8 can be fed to the text reader.
     */
    private static byte[] ionText(Object... parts) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Object part : parts) {
            if (part instanceof String) {
                out.write(((String) part).getBytes("US-ASCII"));
            } else {
                out.write((Integer) part);
            }
        }
        return out.toByteArray();
    }

    private static void readFully(IonReader reader) {
        IonType type;
        while ((type = reader.next()) != null) {
            switch (type) {
                case LIST:
                case SEXP:
                case STRUCT:
                    reader.stepIn();
                    readFully(reader);
                    reader.stepOut();
                    break;
                case STRING:
                case SYMBOL:
                    reader.stringValue();
                    break;
                default:
                    break;
            }
        }
    }

    private static void expectRejected(byte[] data) {
        assertThrows(IonException.class, () -> readFully(IonReaderBuilder.standard().build(data)));
    }

    private static String readOneString(byte[] data) {
        IonReader reader = IonReaderBuilder.standard().build(data);
        reader.next();
        String value = reader.stringValue();
        assertNull(reader.next());
        return value;
    }

    @Test
    public void truncatedSequenceDoesNotConsumeTheTokenTerminator() throws Exception {
        // Without continuation byte validation the 0xC3 lead byte absorbs the closing
        // quote, so this three element list is read as a two element list.
        expectRejected(ionText("[\"", 0xC3, "\"", "x", "\",\"c\"]"));
        expectRejected(ionText("['", 0xC3, "','b']"));
    }

    @Test
    public void truncatedSequenceAtEndOfInputIsRejected() throws Exception {
        expectRejected(ionText("\"", 0xE0, 0xA0));
    }

    @Test
    public void overlongSequencesAreRejected() throws Exception {
        expectRejected(ionText("[\"", 0xC0, 0x80, "\"]"));             // NUL as two bytes
        expectRejected(ionText("[\"", 0xC0, 0xA2, "\"]"));             // '"' as two bytes
        expectRejected(ionText("[\"", 0xE0, 0x80, 0xAF, "\"]"));       // '/' as three bytes
        expectRejected(ionText("[\"", 0xF0, 0x80, 0x80, 0xAF, "\"]")); // '/' as four bytes
    }

    @Test
    public void wellFormedSequencesAreStillAccepted() throws Exception {
        assertEquals("\u00e9", readOneString(ionText("\"", 0xC3, 0xA9, "\"")));
        assertEquals("\u20ac", readOneString(ionText("\"", 0xE2, 0x82, 0xAC, "\"")));
        assertEquals("\ud83d\ude02", readOneString(ionText("\"", 0xF0, 0x9F, 0x98, 0x82, "\"")));
        assertEquals("abc", readOneString(ionText("\"abc\"")));
    }
}
