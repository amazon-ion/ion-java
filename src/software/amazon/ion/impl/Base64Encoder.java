/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import software.amazon.ion.IonException;
import software.amazon.ion.util.IonTextUtils;

/**
 * This is a class that supports encoding and decoding binary
 * data in base 64 encodings.
 * <p>
 * The default encoding is the URL Safe encoding variant specified
 * in http://tools.ietf.org/html/rfc4648
 * <p>
 * It's character translation table is:
 * <pre>
 *    Table 2: The "URL and Filename safe" Base 64 Alphabet
 *
 * NO LONGER URL/FILENAME SAFE, BACK TO THE ORIGINAL URL CHARACTERS
 *
 *   Value Encoding  Value Encoding  Value Encoding  Value Encoding
 *        0 A            17 R            34 i            51 z
 *        1 B            18 S            35 j            52 0
 *        2 C            19 T            36 k            53 1
 *        3 D            20 U            37 l            54 2
 *        4 E            21 V            38 m            55 3
 *        5 F            22 W            39 n            56 4
 *        6 G            23 X            40 o            57 5
 *        7 H            24 Y            41 p            58 6
 *        8 I            25 Z            42 q            59 7
 *        9 J            26 a            43 r            60 8
 *       10 K            27 b            44 s            61 9
 *       11 L            28 c            45 t            62 +
 *       12 M            29 d            46 u            63 /
 *       13 N            30 e            47 v
 *       14 O            31 f            48 w
 *       15 P            32 g            49 x
 *       16 Q            33 h            50 y            (pad) =
 * </pre>
 */
final class Base64Encoder
{
    static class EL {  // EncoderLetter
        public int  value;
        public char letter;
        public EL(int v, char l) {
            value = v;
            letter = l;
        }
    }
    private final static EL[] Base64Alphabet =  {
         new EL((-1), ('='))  // pad
        ,new EL( 0, 'A') ,new EL(17, 'R') ,new EL(34, 'i') ,new EL(51, 'z')
        ,new EL( 1, 'B') ,new EL(18, 'S') ,new EL(35, 'j') ,new EL(52, '0')
        ,new EL( 2, 'C') ,new EL(19, 'T') ,new EL(36, 'k') ,new EL(53, '1')
        ,new EL( 3, 'D') ,new EL(20, 'U') ,new EL(37, 'l') ,new EL(54, '2')
        ,new EL( 4, 'E') ,new EL(21, 'V') ,new EL(38, 'm') ,new EL(55, '3')
        ,new EL( 5, 'F') ,new EL(22, 'W') ,new EL(39, 'n') ,new EL(56, '4')
        ,new EL( 6, 'G') ,new EL(23, 'X') ,new EL(40, 'o') ,new EL(57, '5')
        ,new EL( 7, 'H') ,new EL(24, 'Y') ,new EL(41, 'p') ,new EL(58, '6')
        ,new EL( 8, 'I') ,new EL(25, 'Z') ,new EL(42, 'q') ,new EL(59, '7')
        ,new EL( 9, 'J') ,new EL(26, 'a') ,new EL(43, 'r') ,new EL(60, '8')
        ,new EL(10, 'K') ,new EL(27, 'b') ,new EL(44, 's') ,new EL(61, '9')
        ,new EL(11, 'L') ,new EL(28, 'c') ,new EL(45, 't') ,new EL(62, '+')
        ,new EL(12, 'M') ,new EL(29, 'd') ,new EL(46, 'u') ,new EL(63, '/')
        ,new EL(13, 'N') ,new EL(30, 'e') ,new EL(47, 'v')
        ,new EL(14, 'O') ,new EL(31, 'f') ,new EL(48, 'w')
        ,new EL(15, 'P') ,new EL(32, 'g') ,new EL(49, 'x')
        ,new EL(16, 'Q') ,new EL(33, 'h') ,new EL(50, 'y'),
    };

    final static char  URLSafe64IntToCharTerminator = init64IntToCharTerminator(Base64Alphabet);
    final static int[] URLSafe64IntToChar = init64IntToChar(Base64Alphabet);
    final static int[] URLSafe64CharToInt = init64CharToInt(Base64Alphabet);

    final static int[] Base64EncodingIntToChar = init64IntToChar(Base64Alphabet);
    final static int[] Base64EncodingCharToInt = init64CharToInt(Base64Alphabet);
    final static char  Base64EncodingTerminator = init64IntToCharTerminator(Base64Alphabet);

    static private char init64IntToCharTerminator(EL[] els)
    {
        for (EL letter : els) {
            if (letter.value == -1) {
                return letter.letter;
            }
        }
        throw new RuntimeException(new IonException("fatal: invalid char map definition - missing terminator"));
    }
    static private int[] init64IntToChar(EL[] els)
    {
        int[] output = new int[64];
        for (EL letter : els) {
            if (letter.value != -1) {
                output[letter.value] = letter.letter;
            }
        }
        return output;
    }
    static private int[] init64CharToInt(EL[] els)
    {
        int[] output = new int[256];
        for (int ii=0; ii<256; ii++) {
            output[ii] = -1; // mark everything as invalid to start with
        }
        // mark the valid entries with a non -1 value is they're useful
        for (EL letter : els) {
            if (letter.letter > 255) {
                throw new RuntimeException("fatal base 64 encoding static initializer: letter out of bounds");
            }
            else if (letter.value >= 0) {
                output[letter.letter] = letter.value;
            }
        }
        return output;
    }

    final static boolean isBase64Character(int c) {
        if (c < 32 || c > 255) return false;
        return (URLSafe64CharToInt[c] >= 0);
    }

    private Base64Encoder() {}

    /*********************************************************************
     *
     * BinaryStream, reads a text input and decodes the printable characters
     *               into a binary output stream
     *
     *               reads 1024 characters at a time from the input stream
     *               as there is a 3:4 output to input character ratio
     */
    final static int BUFSIZE = 1024;

    static final class BinaryStream
        extends Reader
    {
        boolean  _ready;
        Reader   _source;
        int[]    _chartobin;
        int      _terminator;
        int      _otherTerminator;
        int      _terminatingChar;
        int      _state;      // 0=started, 1=eof
        char[]   _buffer;
        int      _bufEnd;
        int      _bufPos;

        BinaryStream(Reader input, int[] chartobin, char terminator, char otherTerminator)
        {
            _source = input;
            _chartobin = chartobin;
            _terminator = terminator;
            _otherTerminator = otherTerminator;
            _terminatingChar = -1;
            _buffer = new char[4+1];
            _ready = true;
        }

        BinaryStream(Reader input, char altterminator)
        {
            this(input, URLSafe64CharToInt, URLSafe64IntToCharTerminator, altterminator);
        }

        int terminatingChar()
        {
            return this._terminatingChar;
        }

        private int characterToBinary(final int c) throws IOException {
            int result = -1;
            if (c >= 0 && c < _chartobin.length) {
                result = _chartobin[c];
            }
            if (result < 0) {
                throw new IonException("invalid base64 character (" + c + ")");
            }
            return result;
        }

        // Read a buffer from the input stream and prep the output stream
        private void loadNextBuffer() throws IOException
        {

            int inlen = 0;
            int convert, c = -1, cbin;

            this._bufEnd = 0;
            this._bufPos = 0;

            if (this._state == 1) {
                return;
            }

            // try to read in 4 convertable text characters from the source stream
            while (inlen < 4) {
                c = this._source.read();
                if (c == -1 || c == 65535 || c == this._terminator || c == this._otherTerminator) {
                    _terminatingChar = c;
                    break;
                }
                if (IonTextUtils.isWhitespace(c)) continue;
                cbin = characterToBinary(c);

                this._buffer[inlen++] = (char)cbin;
            }
            if (inlen != 4) {
                if (inlen == 0 && c != this._terminator) {
                    this._state = 1;
                    return;
                }
                // read through the trailing '='s
                int templen = inlen;
                while (c == this._terminator) {
                    templen++;
                    c = this._source.read();
                }
                if (templen != 4) {
                    throw new IonException("base64 character count must be divisible by 4, using '=' for padding");
                }
                else if (inlen < 1) {
                    throw new IonException("base64 character count must be divisible by 4, but using no more than 3 '=' chars for padding");
                }

                this._terminatingChar = c;
            }

            // now convert those characters
            int ii = 0;
            for (;;) {

                // next usable char:
                c = this._buffer[ii++];
                convert =  c << 18;

                // next usable char:
                c = this._buffer[ii++];
                convert |= (c << 12);
                if (inlen < 3) {
                    // with 2 input chars (6*2 = 12 bits) we get only 1 output byte (8 bits)
                    this._buffer[this._bufEnd++] = (char)((convert & 0x00FF0000) >> 16);
                    this._state = 1; // if we ran out, then we're done
                    break;
                }

                // next usable char:
                c = this._buffer[ii++];
                convert |= (c << 6);
                if (inlen < 4) {
                    // with 3 input chars (6*3 = 18 bits) we get 2 output bytes (8*2 = 16 bits)
                    this._buffer[this._bufEnd++] = (char)((convert & 0x00FF0000) >> 16);
                    this._buffer[this._bufEnd++] = (char)((convert & 0x0000FF00) >> 8);
                    this._state = 1; // if we ran out, then we're done
                    break;
                }

                //    next (and final possible out of 4) usable char:
                c = this._buffer[ii++];
                convert |= (c << 0);
                // and with 4 input chars (6*4 = 24 bits) we get the full 3 output byte2 (8*3 = 24 bits)
                this._buffer[this._bufEnd++] = (char)((convert & 0x00FF0000) >> 16);
                this._buffer[this._bufEnd++] = (char)((convert & 0x0000FF00) >> 8);
                this._buffer[this._bufEnd++] = (char)((convert & 0x000000FF) >> 0);
                break;
            }
            return;
        }

        @Override
        public boolean markSupported()
        {
            return false;
        }

        @Override
        public void close() throws IOException
        {
            this._source.close();
        }

        //Read a single character.
        @Override
        public int read() throws IOException
        {
            int outchar = -1;
            if (!_ready) {
                throw new IOException(this.getClass().getName()+ " is not ready");
            }

            // read input until we get a translatable character
            if (this._bufPos >= this._bufEnd) {
                this.loadNextBuffer();
            }
            if (this._bufPos < this._bufEnd) {
                outchar = this._buffer[this._bufPos++];
            }
            return outchar;
        }

        // Read characters into an array.
        @Override
        public int read(char[] cbuf) throws IOException
        {
            int ii = 0;
            for (ii=0; ii<cbuf.length; ii++) {
                int c = read();
                if (c == -1) break;
                cbuf[ii] = (char)c;
            }
            return ii;
        }

        // Read characters into a portion of an array.
        @Override
        public int read(char[] cbuf, int off, int rlen) throws IOException
        {
            int ii = 0;
            for (ii=0; ii<rlen; ii++) {
                int c = read();
                if (c == -1) break;
                cbuf[off + ii] = (char)c;
            }
            return ii;
        }

    }

    final static int BUFSIZE_BIN = 3*BUFSIZE/8; // 1024 -> 384
    final static int BUFSIZE_TEXT = (BUFSIZE/2);  // 1024/2 -> 512 .. (384/3)*4 = 512

    /**
     * The TextStream takes a reader over binary data and returns
     * a text reader.  This reads binary bytes and produces base64
     * encoded printable characters
     */
    static final class TextStream
        extends Reader
    {
        final InputStream _source;
        final int[]    _bintochar;
        final char     _padding;
        boolean  _ready;
        int      _state;      // 0=started, 1=eof
        byte[]   _inbuf  = new byte[BUFSIZE_BIN+1];  // +1 for terminator
        char[]   _outbuf = new char[BUFSIZE_TEXT];
        int      _outBufEnd;
        int      _outBufPos;

        TextStream(InputStream input, int[] bintochar, char terminator)
        {
            _source = input;
            _bintochar = bintochar;
            _padding = terminator;
            _ready = true;
        }

        public TextStream(InputStream input)
        {
            this(input, URLSafe64IntToChar, URLSafe64IntToCharTerminator);
        }

        //Close the stream.
        @Override
        public void close() throws IOException
        {
            if (_ready) {
                _ready = false;
                _source.close();
            }
            this._inbuf = null;
            this._outbuf = null;
        }

        // Mark the present position in the stream.
        @Override
        public void mark(int readAheadLimit) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        // Read a buffer from the binary input stream and prep the output stream
        //
        // note that the output is longer than the input as 3 binary
        // bytes produce 4 ascii characters
        //
        @SuppressWarnings("cast")
        private void loadNextBuffer() throws IOException
        {
            this._outBufEnd = 0;
            this._outBufPos = 0;
            int inlen = this._source.read(this._inbuf, 0, BUFSIZE_BIN);
            if (inlen < 0) {
                this._state = 1;
                return;
            }

            int ii = 0;
            for (;;) {
                // next usable char:
                if (ii >= inlen) {
                    break;
                }
                int c = (((int)this._inbuf[ii++]) & 0xFF);
                int convert =  c << 16;

                // next usable char
                if (ii >= inlen) {
                    this._outbuf[this._outBufEnd++] = (char)this._bintochar[((convert & 0xFC0000) >> 18)];
                    this._outbuf[this._outBufEnd++] = (char)this._bintochar[((convert & 0x03F000) >> 12)];
                    this._outbuf[this._outBufEnd++] = (char)this._padding;
                    this._outbuf[this._outBufEnd++] = (char)this._padding;
                    break;
                }
                c = (((int)this._inbuf[ii++]) & 0xFF);
                convert |=  c << 8;

                // next usable char
                if (ii >= inlen) {
                    this._outbuf[this._outBufEnd++] = (char)this._bintochar[((convert & 0xFC0000) >> 18)];
                    this._outbuf[this._outBufEnd++] = (char)this._bintochar[((convert & 0x03F000) >> 12)];
                    this._outbuf[this._outBufEnd++] = (char)this._bintochar[((convert & 0x000FC0) >>  6)];
                    this._outbuf[this._outBufEnd++] = (char)this._padding;
                    break;
                }
                c = (((int)this._inbuf[ii++]) & 0xFF);
                convert |=  c << 0;

                this._outbuf[this._outBufEnd++] = (char)this._bintochar[((convert & 0xFC0000) >> 18)];
                this._outbuf[this._outBufEnd++] = (char)this._bintochar[((convert & 0x03F000) >> 12)];
                this._outbuf[this._outBufEnd++] = (char)this._bintochar[((convert & 0x000FC0) >>  6)];
                this._outbuf[this._outBufEnd++] = (char)this._bintochar[((convert & 0x00003F) >>  0)];
            }
            return;
        }
        //Read a single character.
        @Override
        public int read() throws IOException
        {
            int outchar = -1;
            if (!_ready) {
                throw new IOException(this.getClass().getName()+ " is closed");
            }
            if (_state != 0) return  -1;

            // read input until we get a translatable character
            if (this._outBufPos >= this._outBufEnd) {
                this.loadNextBuffer();
            }
            if (this._outBufPos < this._outBufEnd) {
                outchar = this._outbuf[this._outBufPos++];
            }
            return outchar;
        }

        // Read characters into an array.
        @Override
        public int read(char[] cbuf) throws IOException
        {
            if (!_ready) {
                throw new IOException(this.getClass().getName()+ " is closed");
            }
            if (_state != 0) return -1;

            int dstPos = 0;
            int needed = cbuf.length;

            while (needed > 0) {
                // read input until we get a translatable character
                if (this._outBufPos >= this._outBufEnd) {
                    this.loadNextBuffer();
                }
                if (this._outBufPos >= this._outBufEnd) {
                    break;
                }
                int xfer = this._outBufEnd - this._outBufPos;
                if (xfer > needed) xfer = needed;
                System.arraycopy(this._outbuf, this._outBufPos, cbuf, dstPos, xfer);
                this._outBufPos += xfer;
                needed -= xfer;
            }
            return dstPos;
        }

        // Read characters into a portion of an array.
        @Override
        public int read(char[] cbuf, int off, int rlen) throws IOException
        {
            if (!_ready) {
                throw new IOException(this.getClass().getName()+ " is closed");
            }
            if (_state != 0) return -1;

            int dstPos = off;
            int needed = rlen;
            while (needed > 0) {
                // read input until we get a translatable character
                if (this._outBufPos >= this._outBufEnd) {
                    this.loadNextBuffer();
                }
                if (this._outBufPos >= this._outBufEnd) {
                    break;
                }
                int xfer = this._outBufEnd - this._outBufPos;
                if (xfer > needed) xfer = needed;
                System.arraycopy(this._outbuf, this._outBufPos, cbuf, dstPos, xfer);
                this._outBufPos += xfer;
                dstPos += xfer;
                needed -= xfer;
            }
            return dstPos;
        }

        // Tell whether this stream is ready to be read.
        @Override
        public boolean ready() throws IOException
        {
            // TODO I don't think this is strictly correct.
            return this._ready && (_source.available() > 0);
        }

        // Reset the stream.
        @Override
        public void reset() throws IOException
        {
            throw new IOException("reset not supported");
        }

        //Skip characters.
        @Override
        public long skip(long n) throws IOException
        {
            if (!_ready) {
                throw new IOException(this.getClass().getName()+ " is closed");
            }
            if (n < 0) {
                throw new IllegalArgumentException("error skip only support non-negative a values for n");
            }
            long needed = n;
            int  available;

            while (needed > 0) {
                available = this._outBufEnd - this._outBufPos;
                if (available < 1) {
                    this.loadNextBuffer();
                    available = this._outBufEnd - this._outBufPos;
                    if (available < 1) break;
                }
                if (available > needed) {
                    this._outBufPos += (int)needed;
                    needed = 0;
                    break;
                }
                needed -= available;
                this._outBufPos += available;
            }
            return n - needed;
        }
    }



}
