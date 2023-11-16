/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.util.LinkedList;

/**
 * Extension of the {@link java.io.PushbackReader} to abstract line/offset counting
 * and push back support.
 *
 * Note that this class does not leverage {@link java.io.LineNumberReader} as we
 * need to un-roll the line number on push back and we need to have the logic to
 * deal with newline combinations anyhow.
 */
final class IonCharacterReader extends PushbackReader {

    /**
     * The default buffer size
     *
     * @see _Private_Utils#MAX_LOOKAHEAD_UTF16
     */
    public static final int DEFAULT_BUFFER_SIZE = 12;

    /**
     * The additional buffer padding--this is to add to the fact that the
     * pushback buffer may have to be larger than user specified characters
     * a CR/LF combo is 2 characters, but logically one newline.
     *
     * FIXME does this properly account for surrogates?
     *
     * @see _Private_Utils#MAX_LOOKAHEAD_UTF16
     */
    public static final int BUFFER_PADDING = 1;

    private long m_consumed;
    private int  m_line;
    private int  m_column;
    private int  m_size;

    // our offset stack--really, for efficiency this should
    // be a linked list of primitive ints to avoid boxing costs
    // however, pursue this only if it is really a performance
    // issue.  Also, we are not using the List interface for code
    // clarity as this is used as a deque. (Java 6 has java.util.Deque)

    /**
     * This offset stack is for pushing back characters that unroll lines
     * and keeps our offset/line number counts correct.  This is essentially
     * a pushback stack for character offsets.
     */
    private LinkedList<Integer> m_columns;

    /**
     * Constructs a character reader with an explicit buffer size.  Note that this
     * is a minimum and the implementation is allowed to make it larger for
     * internal operations.
     *
     * @param in    The underlying reader to wrap.
     * @param size  The size of the push back buffer.
     */
    public IonCharacterReader( final Reader in, final int size ) {
        super( in, size + BUFFER_PADDING );

        assert size > 0;

        m_consumed = 0;
        m_line = 1;
        m_column = 0;
        m_columns = new LinkedList<Integer>();
        m_size = size + BUFFER_PADDING;
    }

    /**
     * Constructs a character reader with the default buffer size.
     *
     * @param in    The underlying reader to wrap.
     */
    public IonCharacterReader( final Reader in ) {
        this( in, DEFAULT_BUFFER_SIZE );
    }

    /**
     * Returns the logical number of consumed characters.
     * This does not necessarily equal to the number of
     * actual characters read as newline combinations are
     * treated as one.
     *
     * @return The logical number of consumed characters.
     */
    public final long getConsumedAmount() {
        return m_consumed;
    }

    /**
     * Returns the current line number in the stream based on the last call
     * to read().
     *
     * @return  The line number, 1-based.
     */
    public final int getLineNumber() {
        return m_line;
    }

    /**
     * Returns the offset within the line based on the last call to read().
     *
     * @return  The offset, 1-based.
     */
    public final int getColumn() {
        return m_column;
    }

    /**
     * Uses the push back implementation but normalizes newlines to "\n".
     */
    @Override
    public int read() throws IOException {
        int nextChar = super.read();

        // process the character
        if ( nextChar != -1 ) {
            if ( nextChar == '\n' ) {
                m_line++;
                pushColumn( m_column );
                m_column = 0;
            }
            else if ( nextChar == '\r') {
                int aheadChar = super.read();

                // if the lookahead is not a newline combo, unread it
                if ( aheadChar != '\n' ) {
                    // no need to unread it with line/offset update.
                    unreadImpl( aheadChar, false );
                }

                m_line++;
                pushColumn( m_column );
                m_column = 0;

                // normalize
                nextChar = '\n';
            } else {
                m_column++;
            }
            m_consumed++;
        }

        return nextChar;
    }

    private final void pushColumn( final int offset ) {
        // constrain the offset stack
        // to the buffer size
        if ( m_columns.size() == m_size ) {
            m_columns.removeFirst();
        }

        // box into collection
        m_columns.addLast( offset );
    }

    private final int popColumn() throws IOException {
        if ( m_columns.isEmpty() ) {
            throw new IOException( "Cannot unread past buffer" );
        }

        // unbox out of collection
        return m_columns.removeLast();
    }

    /**
     * Readers a buffer's worth of data.  This implementation
     * simply leverages {@link #read()} over the buffer.
     */
    @Override
    public int read( char[] cbuf, int off, int len ) throws IOException {
        assert len >= 0;
        assert off >= 0;

        int amountRead = 0;
        final int endIndex = off + len;
        for ( int index = off; index < endIndex; index++ ) {
            int readChar = read();
            if ( readChar == -1 ) {
                break;
            }

            cbuf[ index ] = ( char ) readChar;
            amountRead++;
        }

        return amountRead == 0 ? -1 : amountRead;
    }

    /**
     * Skips over some number of characters.
     * This is implemented as a series of {@link #read()} calls.
     */
    @Override
    public long skip( final long n ) throws IOException {
        assert n > 0;

        long charsLeft = n;
        // note the read side effect
        while ( charsLeft > 0 && read() != -1 ) {
            charsLeft--;
        }
        return n - charsLeft;
    }

    /**
     * Delegates to {@link #unread(int)}.
     */
    @Override
    public void unread( char[] cbuf, int off, int len ) throws IOException {
        assert len >= 0;
        assert off >= 0;

        final int endIndex = off + len;
        for ( int index = endIndex - 1; index >= off; index-- ) {
            unread( cbuf[ index ] );
        }
    }

    /**
     * Delegates to {@link #unread(char[],int,int)}.
     */
    @Override
    public void unread( char[] cbuf ) throws IOException {
        unread( cbuf, 0, cbuf.length );
    }

    /**
     * Will unread a character and update the line number if necessary.  This will throw an
     * exception if a carriage return is given as this character is never yielded from
     * this stream.
     */
    @Override
    public void unread( int c ) throws IOException {
        if ( c == '\r' ) {
            throw new IOException( "Cannot unread a carriage return" );
        }

        unreadImpl( c, true );
    }

    /**
     * Performs ths actual unread operation.
     *
     * @param c the character to unread.
     * @param updateCounts Whether or not we actually update the line number.
     */
    private void unreadImpl(int c, boolean updateCounts ) throws IOException {
        if ( c != -1 ) {
            if ( updateCounts ) {
                if ( c == '\n' ) {
                    m_line--;
                    m_column = popColumn();
                } else {
                    m_column--;
                }
                m_consumed--;
            }
            super.unread( c );
        }
    }

}
