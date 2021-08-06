package com.amazon.ion.impl.bin.utf8;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

/**
 * Encodes {@link String}s to UTF-8. Instances of this class are reusable but are NOT threadsafe.
 *
 * Instances are vended by {@link Utf8StringEncoderPool#getOrCreate()}.
 *
 * {@link #encode(String)} can be called any number of times. Users are expected to call {@link #close()} when
 * the encoder is no longer needed.
 */
public class Utf8StringEncoder extends Poolable<Utf8StringEncoder> {
    // The longest String (as measured by {@link java.lang.String#length()}) that this instance can encode without
    // requiring additional allocations.
    private static final int SMALL_STRING_SIZE = 4 * 1024;

    // Reusable resources for encoding Strings as UTF-8 bytes
    final CharsetEncoder utf8Encoder;
    final ByteBuffer utf8EncodingBuffer;
    final char[] charArray;
    final CharBuffer charBuffer;

    Utf8StringEncoder(Pool<Utf8StringEncoder> pool) {
        super(pool);
        utf8Encoder = Charset.forName("UTF-8").newEncoder();
        utf8EncodingBuffer = ByteBuffer.allocate((int) (SMALL_STRING_SIZE * utf8Encoder.maxBytesPerChar()));
        charArray = new char[SMALL_STRING_SIZE];
        charBuffer = CharBuffer.wrap(charArray);
    }

    /**
     * Encodes the provided String's text to UTF-8. Unlike {@link String#getBytes(Charset)}, this method will not
     * silently replace characters that cannot be encoded with a substitute character. Instead, it will throw
     * an {@link IllegalArgumentException}.
     *
     * Some resources in the returned {@link Result} may be reused across calls to this method. Consequently,
     * callers should use the Result and discard it immediately.
     *
     * @param text A Java String to encode as UTF8 bytes.
     * @return  A {@link Result} containing a byte array of UTF-8 bytes and encoded length.
     * @throws IllegalArgumentException if the String cannot be encoded as UTF-8.
     */
    public Result encode(String text) {
        /*
         This method relies on the standard CharsetEncoder class to encode each String's UTF-16 char[] data into
         UTF-8 bytes. Strangely, CharsetEncoders cannot operate directly on instances of a String. The CharsetEncoder
         API requires all inputs and outputs to be specified as instances of java.nio.ByteBuffer and
         java.nio.CharBuffer, making some number of allocations mandatory. Specifically, for each encoding operation
         we need to have:

            1. An instance of a UTF-8 CharsetEncoder.
            2. A CharBuffer representation of the String's data.
            3. A ByteBuffer into which the CharsetEncoder may write UTF-8 bytes.

         To minimize the overhead involved, the Utf8StringEncoder will reuse previously initialized resources wherever
         possible. However, because CharBuffer and ByteBuffer each have a fixed length, we can only reuse them for
         Strings that are small enough to fit. This creates two kinds of input String to encode: those that are small
         enough for us to reuse our buffers ("small strings"), and those which are not ("large strings").

         The String#getBytes(Charset) method cannot be used for two reasons:

               1. It always allocates, so we cannot reuse any resources.
               2. If/when it encounters character data that cannot be encoded as UTF-8, it simply replaces that data
                 with a substitute character[1]. (Sometimes seen in applications as a '?'.) In order
                 to surface invalid data to the user, the method must be able to detect these events at encoding time.

            [1] https://en.wikipedia.org/wiki/Substitute_character
        */

        CharBuffer stringData;
        ByteBuffer encodingBuffer;

        int length = text.length();

        // While it is technically possible to encode any String using a fixed-size encodingBuffer, we need
        // to be able to write the length of the complete UTF-8 string to the output stream before we write the string
        // itself. For simplicity, we reuse or create an encodingBuffer that is large enough to hold the full string.

        // In order to encode the input String, we need to pass it to CharsetEncoder as an implementation of CharBuffer.
        // Surprisingly, the intuitive way to achieve this (the CharBuffer#wrap(CharSequence) method) adds a large
        // amount of CPU overhead to the encoding process. Benchmarking shows that it's substantially faster
        // to use String#getChars(int, int, char[], int) to copy the String's backing array and then call
        // CharBuffer#wrap(char[]) on the copy.

        if (length > SMALL_STRING_SIZE) {
            // Allocate a new buffer for large strings
            encodingBuffer = ByteBuffer.allocate((int) (text.length() * utf8Encoder.maxBytesPerChar()));
            char[] chars = new char[text.length()];
            text.getChars(0, text.length(), chars, 0);
            stringData = CharBuffer.wrap(chars);
        } else {
            // Reuse our existing buffers for small strings
            encodingBuffer = utf8EncodingBuffer;
            encodingBuffer.clear();
            stringData = charBuffer;
            text.getChars(0, text.length(), charArray, 0);
            charBuffer.rewind();
            charBuffer.limit(text.length());
        }

        // Because encodingBuffer is guaranteed to be large enough to hold the encoded string, we can
        // perform the encoding in a single call to CharsetEncoder#encode(CharBuffer, ByteBuffer, boolean).
        CoderResult coderResult = utf8Encoder.encode(stringData, encodingBuffer, true);

        // 'Underflow' is the success state of a CoderResult.
        if (!coderResult.isUnderflow()) {
            throw new IllegalArgumentException("Could not encode string as UTF8 bytes: " + text);
        }
        encodingBuffer.flip();
        int utf8Length = encodingBuffer.remaining();

        // In most usages, the JVM should be able to eliminate this allocation via an escape analysis of the caller.
        return new Result(utf8Length, encodingBuffer.array());
    }

    /**
     * Represents the result of a {@link Utf8StringEncoder#encode(String)} operation.
     */
    public static class Result {
        final private byte[] buffer;
        final private int encodedLength;

        public Result(int encodedLength, byte[] buffer) {
            this.encodedLength = encodedLength;
            this.buffer = buffer;
        }

        /**
         * Returns a byte array containing the encoded UTF-8 bytes starting at index 0. This byte array is NOT
         * guaranteed to be the same length as the data it contains. Callers must use {@link #getEncodedLength()}
         * to determine the number of bytes that should be read from the byte array.
         *
         * @return the buffer containing UTF-8 bytes.
         */
        public byte[] getBuffer() {
            return buffer;
        }

        /**
         * @return the number of encoded bytes in the array returned by {@link #getBuffer()}.
         */
        public int getEncodedLength() {
            return encodedLength;
        }
    }
}
