package com.amazon.ion.impl.bin.utf8;

import com.amazon.ion.IonException;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

/**
 * Decodes {@link String}s from UTF-8. Instances of this class are reusable but are NOT threadsafe.
 *
 * Instances are vended by {@link Utf8StringDecoderPool#getOrCreate()}.
 *
 * Users are expected to call {@link #close()} when the decoder is no longer needed.
 *
 * There are two ways of using this class:
 * <ol>
 *     <li>Use {@link #decode(ByteBuffer, int)} to decode the requested number of bytes from the given ByteBuffer in
 *     a single step. Or,</li>
 *     <li>Use the following sequence of method calls:
 *     <ol>
 *         <li>{@link #prepareDecode(int)} to prepare the decoder to decode the requested number of bytes.</li>
 *         <li>{@link #partialDecode(ByteBuffer, boolean)} to decode the available bytes from the byte buffer. This may
 *         be repeated as more bytes are made available in the ByteBuffer, which is the caller's responsibility.</li>
 *         <li>{@link #finishDecode()} to finish decoding and return the resulting String.</li>
 *     </ol>
 *     Note: {@link #decode(ByteBuffer, int)} must not be called between calls to {@link #prepareDecode(int)} and
 *     {@link #finishDecode()}.
 *     </li>
 * </ol>
 */
public class Utf8StringDecoder extends Poolable<Utf8StringDecoder> {

    // The size of the UTF-8 decoding buffer.
    private static final int UTF8_BUFFER_SIZE_IN_BYTES = 4 * 1024;

    private final CharBuffer reusableUtf8DecodingBuffer;
    private final CharsetDecoder utf8CharsetDecoder;
    private CharBuffer utf8DecodingBuffer;

    Utf8StringDecoder(Pool<Utf8StringDecoder> pool) {
        super(pool);
        reusableUtf8DecodingBuffer = CharBuffer.allocate(UTF8_BUFFER_SIZE_IN_BYTES);
        utf8CharsetDecoder = Charset.forName("UTF-8").newDecoder();
    }

    /**
     * Prepares the decoder to decode the given number of UTF-8 bytes.
     * @param numberOfBytes the number of bytes to decode.
     */
    public void prepareDecode(int numberOfBytes) {
        utf8CharsetDecoder.reset();
        utf8DecodingBuffer = reusableUtf8DecodingBuffer;
        if (numberOfBytes > reusableUtf8DecodingBuffer.capacity()) {
            utf8DecodingBuffer = CharBuffer.allocate(numberOfBytes);
        }
    }

    /**
     * Decodes the available bytes from the given ByteBuffer.
     * @param utf8InputBuffer a ByteBuffer containing UTF-8 bytes.
     * @param endOfInput true if the end of the UTF-8 sequence is expected to occur in the buffer; otherwise, false.
     */
    public void partialDecode(ByteBuffer utf8InputBuffer, boolean endOfInput) {
        CoderResult coderResult = utf8CharsetDecoder.decode(utf8InputBuffer, utf8DecodingBuffer, endOfInput);
        if (coderResult.isError()) {
            throw new IonException("Illegal value encountered while validating UTF-8 data in input stream. " + coderResult.toString());
        }
    }

    /**
     * Finishes decoding and returns the resulting String.
     * @return the decoded Java String.
     */
    public String finishDecode() {
        utf8DecodingBuffer.flip();
        return utf8DecodingBuffer.toString();
    }

    /**
     * Decodes the given number of UTF-8 bytes from the given ByteBuffer into a Java String.
     * @param utf8InputBuffer a ByteBuffer containing UTF-8 bytes.
     * @param numberOfBytes the number of bytes from the utf8InputBuffer to decode.
     * @return the decoded Java String.
     */
    public String decode(ByteBuffer utf8InputBuffer, int numberOfBytes) {
        prepareDecode(numberOfBytes);

        utf8DecodingBuffer.position(0);
        utf8DecodingBuffer.limit(utf8DecodingBuffer.capacity());

        partialDecode(utf8InputBuffer, true);
        return finishDecode();
    }
}
