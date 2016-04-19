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


abstract class UnifiedInputBufferX
{
    public enum BufferType { BYTES, CHARS }

    protected int               _page_size;
    protected UnifiedDataPageX[] _buffers;
    protected int               _buffer_current;
    protected int               _buffer_count;
    protected int               _locks;

    public static UnifiedInputBufferX makePageBuffer(byte[] bytes, int offset, int length) {
        UnifiedInputBufferX buf = new UnifiedInputBufferX.Bytes(bytes, offset, length);
        return buf;
    }
    public static UnifiedInputBufferX makePageBuffer(char[] chars, int offset, int length) {
        UnifiedInputBufferX buf = new UnifiedInputBufferX.Chars(chars, offset, length);
        return buf;
    }
    public static UnifiedInputBufferX makePageBuffer(CharSequence chars, int offset, int length) {
        char [] char_array = chars_make_char_array(chars, offset, length);
        UnifiedInputBufferX buf = makePageBuffer(char_array, 0, length);
        return buf;
    }
    public static UnifiedInputBufferX makePageBuffer(BufferType bufferType, int initialPageSize)
    {
        UnifiedInputBufferX buf;
        switch(bufferType) {
        case CHARS:
            buf = new UnifiedInputBufferX.Chars(initialPageSize);
            break;
        case BYTES:
            buf = new UnifiedInputBufferX.Bytes(initialPageSize);
            break;
        default:
            throw new IllegalArgumentException("invalid buffer type");
        }
        return buf;
    }
    protected static final char[] chars_make_char_array(CharSequence chars,
                                                         int offset,
                                                         int length)
    {
        char[] char_array = new char[length];
        for (int ii=offset; ii<length; ii++) {
            char_array[ii] = chars.charAt(ii);
        }
        return char_array;
    }
    private UnifiedInputBufferX(int initialPageSize) {
        if (initialPageSize < 0) {
            throw new IllegalArgumentException("page size must be > 0");
        }
        _page_size = initialPageSize;
        _buffers = new UnifiedDataPageX[10];
    }

    public abstract BufferType getType();
    public abstract int maxValue();

    public final void putCharAt(long fileOffset, int c) {
        if (c < 0 || c > maxValue()) throw new IllegalArgumentException("value ("+c+")is out of range (0 to "+maxValue()+")");

        // since we start at _curr the common case find the buffer immediately
        UnifiedDataPageX page = null;
        for (int ii=_buffer_current; ii>=0; ii--) {
            if (_buffers[ii].containsOffset(fileOffset)) {
                page = _buffers[ii];
                break;
            }
        }
        if (page == null) throw new IllegalArgumentException();
        int offset = (int)(fileOffset - page.getStartingFileOffset());
        page.putValue(offset, c);
    }

    public final UnifiedDataPageX getCurrentPage() {
        return _buffers[_buffer_current];
    }

    public final int getCurrentPageIdx()   {
        return _buffer_current;
    }

    public final int getPageCount() {
        return _buffer_count;
    }

    public final void incLock() {
        _locks++;
    }
    public final boolean decLock() {
        _locks--;
        return (_locks == 0);
    }

    public final UnifiedDataPageX getPage(int pageIdx) {
        if (pageIdx < 0 || pageIdx >= _buffer_count) {
            throw new IndexOutOfBoundsException();
        }
        return _buffers[pageIdx];
    }

    protected final int getNextFilledPageIdx() {
        int idx = _buffer_current + 1;

        if (idx < _buffer_count) {
            UnifiedDataPageX p = _buffers[idx];
            if (p != null) {
                _buffer_current = idx;
                return idx;
            }
        }
        return -1;
    }

    protected final UnifiedDataPageX getEmptyPageIdx() {
        UnifiedDataPageX next = null;

        if (_buffer_count < _buffers.length) {
            next = _buffers[_buffer_count];
        }
        if (next == null) {
            next = make_page(_page_size);
        }
        else {
            assert(_buffer_count == (_buffer_current + 1));
        }
        return next;
    }

    abstract protected UnifiedDataPageX make_page(int page_size);

    protected final UnifiedDataPageX setCurrentPage(int idx, UnifiedDataPageX curr) {
        setPage(idx, curr, true);
        if (idx != _buffer_current) {
            _buffer_current = idx;
            if (idx >= _buffer_count) {
                _buffer_count = idx + 1;
            }
        }
        UnifiedDataPageX p = _buffers[idx];
        return p;
    }

    protected final void setPage(int idx, UnifiedDataPageX curr, boolean recycleOldPage)
    {
        int oldlen = _buffers.length;
        if (idx >= oldlen) {
            int newlen = oldlen * 2;
            UnifiedDataPageX[] newbuf = new UnifiedDataPageX[newlen];
            System.arraycopy(_buffers, 0, newbuf, 0, oldlen);
            _buffers = newbuf;
        }
        UnifiedDataPageX prev = _buffers[idx];
        _buffers[idx] = curr;
        if (idx >= _buffer_count) {
            _buffer_count = idx + 1;
        }
        // if the caller wants us to we'll hold onto this
        // page for a bit, since it's now available
        if (recycleOldPage
         && prev != null
         && prev != curr
         && (idx + 1) < _buffers.length // it's not worth reallocating the _buffers array for this
         ) {
            _buffers[idx+1] = prev;
        }
    }

    /**
     * resets the buffer list to start at the current page
     * this releases any "extra" pages.  This does hold
     * on to 1 extra page, if there is one, since two pages
     * if a common occurrence for values that cross the
     * page boundary.
     *
     * when this exits there will be either just the current
     * page in the buffer list or the current page and 1
     * preallocated page just after it (in idx 1).
     */
    protected final void resetToCurrentPage()
    {
        int p0_idx = getCurrentPageIdx();
        if (p0_idx > 0) {
            // this is a common "do nothing" case, it happens
            // when we reset to the page and there are no saved
            // pages - i.e. the mark is contained in curr
            release_pages_to(p0_idx);
        }
    }

    private final void release_pages_to(int p0_idx) {
        assert(p0_idx > 0);

        // we'll try to save the now unneeded 0th empty page
        UnifiedDataPageX empty_page = _buffers[0];

        // now bump all the page ptrs down to release the
        // pages that precede the current page
        int dst = 0;
        int src = p0_idx;
        while (src<_buffer_count) {
            _buffers[dst++] = _buffers[src++];
        }

        // clear any trailing page ptrs
        int end = _buffer_count + 1; // we may have an extra page ptr saved at buffer[count]
        if (end >= _buffers.length) {
            end = _buffers.length;
        }
        while (dst < end) {
            _buffers[dst++] = null;
        }

        _buffer_current -= p0_idx;
        _buffer_count -= p0_idx;
        _buffers[_buffer_count] = empty_page;
    }

    /**
     * this clears all the pages out, except to save
     * one page (unallocated)
     *
     */
    protected final void clear() {
        UnifiedDataPageX curr = getCurrentPage();

        for (int ii=0; ii<_buffers.length; ii++) {
            _buffers[ii] = null;
        }

        // curr is null when underlying stream is empty.
        if (curr != null)
        {
            _buffers[0] = curr;
            curr.reset(0);
        }

        _buffer_count = 0;
        _buffer_current = 0;
    }

    static class Bytes extends UnifiedInputBufferX {
        protected Bytes(int initialPageSize) {
            super(initialPageSize);
        }
        protected Bytes(byte[] bytes, int offset, int length) {
            super(length);
            _buffers[0] = new UnifiedDataPageX.Bytes(bytes, offset, length);
            _buffer_current = 0;
            _buffer_count = 1;
        }
        @Override
        public final BufferType getType() { return BufferType.BYTES; }

        @Override
        protected final UnifiedDataPageX make_page(int page_size) {
            UnifiedDataPageX p = new UnifiedDataPageX.Bytes(page_size);
            return p;
        }

        @Override
        public final int maxValue() { return 0xff; }

    }
    static class Chars extends UnifiedInputBufferX {
        protected Chars(int initialPageSize) {
            super(initialPageSize);
        }
        /** Retains a reference to the chars array! */
        protected Chars(char[] chars, int offset, int length) {
            super(offset + length);
            _buffers[0] = new UnifiedDataPageX.Chars(chars, offset, length);
            _buffer_current = 0;
            _buffer_count = 1;

        }
        /**
         * Makes a copy of the {@link CharSequence}.
         */
        protected Chars(CharSequence chars, int offset, int length) {
            this(chars_make_char_array(chars, offset, length), 0, length);
        }
        @Override
        public final BufferType getType() { return BufferType.CHARS; }

        @Override
        protected final UnifiedDataPageX make_page(int page_size) {
            UnifiedDataPageX p = new UnifiedDataPageX.Chars(page_size);
            return p;
        }

        @Override
        public final int maxValue() { return 0xffff; }
    }
}
