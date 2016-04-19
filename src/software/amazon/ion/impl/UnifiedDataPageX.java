/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

/**
 * this base class and it's two children (below) manage
 *
 */
abstract class UnifiedDataPageX
{
    public enum PageType { BYTES, CHARS }

    protected PageType  _page_type;
    protected int       _page_limit;  // offset of the last filled array element + 1
    protected int       _base_offset; // reserves space for un-reading, or offset of the first valid array element
    protected int       _unread_count;// number of chars the base has been adjusted due to unreading before the user data
    protected long      _file_offset; // offset of the first byte of this buffer (_base_offset ignored) in the input stream

    protected byte[]    _bytes;
    protected char[]    _characters;


    public static final UnifiedDataPageX makePage(byte[] bytes, int offset, int length) {
        return new Bytes(bytes, offset, length);
    }
    public static final UnifiedDataPageX makePage(char[] chars, int offset, int length) {
        return new Chars(chars, offset, length);
    }
    public static final UnifiedDataPageX makePage(PageType pageType, int size) {
        if (size < 1) throw new IllegalArgumentException("invalid page size must be > 0");
        switch (pageType) {
        case CHARS: return new Chars(size);
        case BYTES: return new Bytes(size);
        default: throw new IllegalArgumentException("invalid page type, s/b 1 or 2");
        }
    }

    // this class should only be constructed through the factory
    // methods makePage() which will choose the right type of
    // page to construct
    private UnifiedDataPageX() {}

    public abstract int      getValue(int pageOffset);
    public abstract void     putValue(int pageOffset, int c);
    public final    PageType getPageType() { return _page_type; }
    public final    char[]   getCharBuffer() { return _characters; }
    public final    byte[]   getByteBuffer() { return _bytes; }

    private final boolean isBytes() {
        return (_page_type == PageType.BYTES);
    }

    int load(Reader reader, int start_offset, long file_position) throws IOException
    {
        if (isBytes()) {
            throw new UnsupportedOperationException("byte pages can't load characters");
        }
        int read = reader.read(_characters, start_offset, _characters.length - start_offset);
        if (read > 0) {
            _page_limit = start_offset + read;
            _base_offset = start_offset;
            _unread_count = 0;
            setFilePosition(file_position, start_offset);
        }
        return read;
    }

    int load(InputStream stream, int start_offset, long file_position) throws IOException
    {
        if (!isBytes()) {
            throw new UnsupportedOperationException("character pages can't load bytes");
        }
        int read = stream.read(_bytes, start_offset, _bytes.length - start_offset);
        if (read > 0) {
            _base_offset = start_offset;
            _unread_count = 0;
            _page_limit = start_offset + read;
            setFilePosition(file_position, start_offset);
        }
        return read;
    }

    public int getBufferLimit()    { return _page_limit; }
    public int getOriginalStartingOffset() { return _base_offset; }

    // FIXME document lower-bound of the result.  Can it be negative?
    // If not, why not?
    public int getStartingOffset() { return _base_offset - _unread_count; }
    public int getUnreadCount()    { return _unread_count; }

    public void inc_unread_count() {
        // we need this to handle the calculation of
        // curr_pos - offset when we're marking pages
        _unread_count++;
    }

    public final void setFilePosition(long fileOffset, int pos) {
        if (fileOffset < 0) {
            throw new IllegalArgumentException();
        }
        _file_offset = fileOffset - pos;
        return;
    }

    public final long getFilePosition(int pos) {
        return _file_offset + pos;
    }

    public final int getOffsetOfFilePosition(long filePosition) {
        if (!containsOffset(filePosition)) {
            String message = "requested file position ["
                           + Long.toString(filePosition)
                           + "] is not in this page ["
                           + Long.toString(getStartingFileOffset())
                           + "-"
                           + Long.toString(this.getFilePosition(_page_limit))
                           + "]";
            throw new IllegalArgumentException(message);
        }
        return (int)(filePosition - _file_offset);
    }

    public final long getStartingFileOffset() {
        return _file_offset + _base_offset;
    }

    public final boolean containsOffset(long filePosition) {
        if (_file_offset + _base_offset > filePosition) return false;
        if (filePosition >= _file_offset + _page_limit) return false;
        return true;
    }
    protected final int getLengthFollowingFilePosition(long filePosition) {
        int pos = getOffsetOfFilePosition(filePosition);
        return _page_limit - pos;
    }

    public final void reset(int baseOffset) {
        _base_offset = baseOffset;
        _page_limit = _base_offset;
    }

    public abstract int readFrom(int pageOffset, byte[] bytes, int offset, int length);
    public abstract int readFrom(int pageOffset, char[] chars, int offset, int length);

    /**
     * Specialized versions of DataPage.  One to handle bytes the
     * other to handle chars
     */
    // FIXME: remove "public" when UnifiedOutputBufferX is
    //        integrated back into ion.impl
    public static final class Bytes extends UnifiedDataPageX
    {
        public Bytes(int size) {
            this(new byte[size], 0, size);
        }
        public Bytes(byte[] bytes, int offset, int len) {
            _page_type   = PageType.BYTES;
            _bytes       = bytes;
            _base_offset = offset;
            _page_limit  = offset + len;
        }

        @Override
        public int getValue(int offset) {
            return (_bytes[offset] & 0xff);
        }

        @Override
        public void putValue(int offset, int b) {
           _bytes[_base_offset] = (byte)b;
        }

        @Override
        public final int readFrom(int pageOffset, byte[] bytes, int offset, int length) {
            int bytes_read = length;
            if (pageOffset >= _page_limit) return -1;

            if (bytes_read > _page_limit - pageOffset) {
                bytes_read = _page_limit - pageOffset;
            }
            System.arraycopy(_bytes, pageOffset, bytes, offset, bytes_read);

            return bytes_read;
        }
        @Override
        public final int readFrom(int pageOffset, char[] chars, int offset, int length) {
            throw new UnsupportedOperationException("byte pages can't read characters");
        }
    }

    // FIXME: remove "public" when UnifiedOutputBufferX is
    //        integrated back into ion.impl
    public static final class Chars extends UnifiedDataPageX
    {
        public Chars(int size) {
            this(new char[size], 0, size);
        }
        public Chars(char[] chars, int offset, int len) {
            _page_type   = PageType.CHARS;
            _characters = chars;
            _base_offset = offset;
            _page_limit  = offset + len;
        }

        @Override
        public int getValue(int pageOffset) {
            if (pageOffset < 0 || pageOffset > _page_limit - _base_offset) {
                throw new IllegalArgumentException("offset "+pageOffset+" is not contained in page, limit is "+(_page_limit - _base_offset));
            }
            return _characters[pageOffset];
        }

        @Override
        public void putValue(int pageOffset, int c) {
            _characters[pageOffset] = (char)c;
        }

        @Override
        public final int readFrom(int pageOffset, byte[] bytes, int offset, int length) {
            throw new UnsupportedOperationException("character pages can't read bytes");
        }

        @Override
        public final int readFrom(int pageOffset, char[] chars, int offset, int length)
        {
            int chars_read = length;
            if (pageOffset >= _page_limit) return -1;

            if (chars_read > _page_limit - pageOffset) {
                chars_read = _page_limit - pageOffset;
            }
            System.arraycopy(_characters, pageOffset, chars, offset, chars_read);

            return chars_read;
        }
    }
}
