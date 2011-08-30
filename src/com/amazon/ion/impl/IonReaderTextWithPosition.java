// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonException;

import com.amazon.ion.IonType;

import com.amazon.ion.TextSpan;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.Span;

/**
 *
 */
public class IonReaderTextWithPosition
    extends IonReaderTextUserX
    implements IonReaderWithPosition
{
    static class IonReaderTextPosition
        extends IonReaderPositionBase
        implements TextSpan
    {
        private long    _start_char_offset;
        private IonType _container_type;

        private long    _start_line;
        private long    _start_column;

        IonReaderTextPosition(IonReaderTextWithPosition reader)
        {
            if (reader._scanner.isBufferedInput() == false) {
                throw new IonException("span capable text reader is currently supported only over buffered input");
            }
            // TODO: convert _start_char_offset from a long
            //       to a reference into the Unified* data source
            _start_char_offset = reader._value_start_offset;
            _container_type = reader.getContainerType();

            _start_line = reader.getLineNumber();
            _start_column = reader.getLineOffset();
        }

        public long getStartLine()
        {
            if (_start_line < 1) {
                throw new IllegalStateException("not positioned on a reader");
            }
            return _start_line;
        }

        public long getStartColumn()
        {
            if (_start_column < 1) {
                throw new IllegalStateException("not positioned on a reader");
            }
            return _start_column;
        }

        public long getFinishLine()
        {
            throw new RuntimeException("E_NOT_IMPL - line and column position is not yet available");
        }

        public long getFinishColumn()
        {
            throw new RuntimeException("E_NOT_IMPL - line and column position is not yet available");
        }

        long getStartPosition()
        {
            return _start_char_offset;
        }

        IonType getContainerType() {
            return _container_type;
        }
    }

    /**
     * @param system
     * @param catalog
     * @param chars
     * @param offset
     * @param length
     */
    protected IonReaderTextWithPosition(IonSystem  system,
                                        IonCatalog catalog,
                                        char[]     chars,
                                        int        offset,
                                        int        length
    ) {
        super(system, catalog, chars, offset, length);
    }

    /**
     * @param system
     * @param catalog
     */
    protected IonReaderTextWithPosition(IonSystem  system,
                                        IonCatalog catalog,
                                        String     chars
    ) {
        this(system, catalog, chars.toCharArray(), 0, chars.length());
    }

    public Span currentSpan()
    {
        return this.getCurrentPosition();
    }

    public IonReaderPosition getCurrentPosition()
    {
        IonReaderTextPosition pos = new IonReaderTextPosition(this);
        return pos;
    }

    public void hoist(Span span)
    {
        if (!(span instanceof IonReaderTextPosition)) {
            throw new IllegalArgumentException("position must match the reader");
        }
        IonReaderTextPosition text_span = (IonReaderTextPosition)span;

        UnifiedInputStreamX current_stream = _scanner.getSourceStream();
        char[] chars = current_stream.getCharArray();
        if (chars == null) {
            throw new RuntimeException("E_NOT_IMPL - currently text spans are only supported over char[] and String");
        }

        // we're going to cast this value down.  Since we only support
        // in memory single buffered chars here this is ok.
        assert(text_span.getStartPosition() <= Integer.MAX_VALUE);

        // TODO: this is a pretty expensive way to do this. UnifiedInputStreamX
        //       needs to have a reset method added that can reset the position
        //       and length of the input to be some subset of the original source.
        //       This would avoid a lot of object creation (and wasted destruction.
        //       But this is a time-to-market solution here.  The change can be
        //       made as support for streams is added.
        UnifiedDataPageX curr = current_stream._buffer.getCurrentPage();
        UnifiedInputStreamX iis = UnifiedInputStreamX.makeStream(
                                        chars
                                      , curr._base_offset + (int)text_span._start_char_offset
                                      , curr._page_limit
                                  );
        this.init(iis, null);
    }

    public void seek(IonReaderPosition position)
    {
        hoist(position);
        return;
    }

}
