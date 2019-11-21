package com.amazon.ion.impl;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.util.RepeatInputStream;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.math.BigDecimal;

import static com.amazon.ion.impl._Private_IonConstants.BINARY_VERSION_MARKER_1_0;
import static org.junit.Assert.assertEquals;

public class IonReaderBinaryRawStringTest {

    @Test
    public void testReadShortStrings() throws Exception {
        // This test constructs some strings with non-ascii text that are short enough to fit in
        // IonBinaryReaderRawX's reusable decoding buffers and then round-trips them.

        String adage = "Brevity is the soul of wit. \uD83D\uDE02"; // Laughing face with tears
        String observation = "What's the deal with airline food? \u2708\uFE0F\uD83C\uDF7D️"; // Airplane/fork+knife+plate
        String litotes = "Not bad. \uD83D\uDC4D"; // Thumbs up

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = IonBinaryWriterBuilder.standard().build(out);

        writer.writeString(adage);
        writer.writeString(observation);
        writer.writeString(litotes);
        writer.close();

        byte[] data = out.toByteArray();

        IonReader reader = IonReaderBuilder.standard().build(data);
        reader.next();
        assertEquals(adage, reader.stringValue());
        reader.next();
        assertEquals(observation, reader.stringValue());
        reader.next();
        assertEquals(litotes, reader.stringValue());
    }

    @Test
    public void testReadLargeStrings() throws Exception {
        // This test constructs some strings with non-ascii text that are large enough to exceed the size
        // of IonBinaryReaderRawX's reusable decoding buffers and then round-trips them.

        String verse = "\uD83C\uDFB6 This is the song that never ends\n" // Musical notes emoji
                + "\uD83C\uDFB5 Yes it goes on and on, my friends!\n"
                + "\uD83C\uDFB6 Some people started singing it not knowing what it was\n"
                + "\uD83C\uDFB5 And they'll continue singing it forever just because...\n";

        final int numberOfVerses = 1024;

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < numberOfVerses; i++) {
            stringBuilder.append(verse);
        }
        String longSong = stringBuilder.toString();

        for (int i = 0; i < numberOfVerses; i++) {
            stringBuilder.append(verse);
        }
        String longerSong = stringBuilder.toString();

        for (int i = 0; i < numberOfVerses; i++) {
            stringBuilder.append(verse);
        }
        String longestSong = stringBuilder.toString();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = IonBinaryWriterBuilder.standard().build(out);

        writer.writeString(longSong);
        writer.writeString(longerSong);
        writer.writeString(longestSong);
        writer.close();

        byte[] songData = out.toByteArray();

        IonReader reader = IonReaderBuilder.standard().build(songData);
        reader.next();
        assertEquals(longSong, reader.stringValue());
        reader.next();
        assertEquals(longerSong, reader.stringValue());
        reader.next();
        assertEquals(longestSong, reader.stringValue());
    }
}
