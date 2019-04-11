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

import com.amazon.ion.IonTestCase;
import java.io.IOException;
import java.io.StringReader;
import org.junit.Test;

/**
 * Test cases for {@link IonCharacterReader}.
 */
public class CharacterReaderTest extends IonTestCase
{
    public static IonCharacterReader createReader( final String data, final int size ) {
        StringReader in = new StringReader( data );
        return new IonCharacterReader( in, size );
    }

    public static IonCharacterReader createReader( final StringBuilder data, final int size ) {
        return createReader( data.toString(), size );
    }

    @Test
    public void testEmpty() throws Exception {
        IonCharacterReader in = createReader( "", 1 );
        assertTrue( in.read() == -1 );
        assertTrue( in.getConsumedAmount() == 0 );
        assertTrue( in.getColumn() == 0 );
        assertTrue( in.getLineNumber() == 1 );
    }

    @Test
    public void testEmptyMultipleRead() throws Exception {
        IonCharacterReader in = createReader( "", 1 );
        assertTrue( in.read() == -1 );
        assertTrue( in.read() == -1 );
    }

    @Test
    public void testEmptyUnreadEOF() throws Exception {
        IonCharacterReader in = createReader( "", 1 );
        assertTrue( in.read() == -1 );
        in.unread( -1 );
        assertTrue( in.read() == -1 );
    }

    @Test
    public void testSingleRead() throws Exception {
        IonCharacterReader in = createReader( "A", 1 );

        int read = in.read();
        assertTrue( read == 'A' );
        assertTrue( in.getConsumedAmount() == 1 );
        assertTrue( in.getColumn() == 1 );
        assertTrue( in.getLineNumber() == 1 );

        assertTrue( in.read() == -1 );
        assertTrue( in.getConsumedAmount() == 1 );
        assertTrue( in.getColumn() == 1 );
        assertTrue( in.getLineNumber() == 1 );
    }

    @Test
    public void testSingleUnread() throws Exception {
        IonCharacterReader in = createReader( "B", 1 );

        int read = in.read();

        in.unread( read );
        assertTrue( in.getConsumedAmount() == 0 );
        assertTrue( in.getColumn() == 0 );
        assertTrue( in.getLineNumber() == 1 );

        assertTrue( in.read() == read );
        assertTrue( in.getConsumedAmount() == 1 );
        assertTrue( in.getColumn() == 1 );
        assertTrue( in.getLineNumber() == 1 );
    }

    @Test
    public void testSingleUnreadCarriageReturn() throws Exception {
        IonCharacterReader in = createReader( "B", 1 );

        try {
            in.unread( '\r' );
            fail();
        } catch ( final IOException e ) {}
    }

    @Test
    public void testEmptyUnread() throws Exception {
        IonCharacterReader in = createReader( "", 1 );

        assertTrue( in.read() == -1 );
        in.unread( 'A' );
        assertTrue( in.read() == 'A' );
        assertTrue( in.read() == -1 );
    }

    @Test
    public void testReadBatchMultiline() throws Exception {
        IonCharacterReader in = createReader( "A\r\nB", 3 );

        char[] buf = new char[ 3 ];
        int num = in.read( buf );

        assertTrue( num == 3 );
        assertTrue( buf[ 0 ] == 'A' );
        assertTrue( buf[ 1 ] == '\n' );
        assertTrue( buf[ 2 ] == 'B' );
        assertTrue( in.read() == -1 );
        assertTrue( in.getConsumedAmount() == 3 );
        assertTrue( in.getColumn() == 1 );
        assertTrue( in.getLineNumber() == 2 );
    }

    @Test
    public void testUnreadBatch() throws Exception {
        IonCharacterReader in = createReader( "ABCD", 3 );

        char[] buf = new char[ 3 ];
        int num = in.read( buf );

        assertTrue( num == 3 );
        assertTrue( in.getConsumedAmount() == 3 );
        assertTrue( in.getColumn() == 3 );
        assertTrue( in.getLineNumber() == 1 );
        assertEquals( new String( buf ), "ABC" );

        in.unread( buf );
        assertTrue( in.getConsumedAmount() == 0 );
        assertTrue( in.getColumn() == 0 );
        assertTrue( in.getLineNumber() == 1 );

        buf = new char[ 3 ];
        in.read( buf );
        assertTrue( num == 3 );
        assertTrue( in.getConsumedAmount() == 3 );
        assertTrue( in.getColumn() == 3 );
        assertTrue( in.getLineNumber() == 1 );
        assertEquals( new String( buf ), "ABC" );
    }

    @Test
    public void testUnreadBatchMultiline() throws Exception {
        IonCharacterReader in = createReader( "A\r\n\n\nB\n\nC\n\nD", 3 );

        in.read(); // Skip the initial 'A'

        char[] buf = new char[ 3 ];
        int num = in.read( buf );  // Consume \r\n\n\n

        assertTrue( num == 3 );
        assertTrue( in.getConsumedAmount() == 4 );
        assertTrue( in.getColumn() == 0 );
        assertTrue( in.getLineNumber() == 4 );
        assertEquals( new String( buf ), "\n\n\n" );

        in.unread( buf );
        assertTrue( in.getConsumedAmount() == 1 );
        assertTrue( in.getColumn() == 1 );
        assertTrue( in.getLineNumber() == 1 );

        buf = new char[ 3 ];
        in.read( buf );
        assertTrue( num == 3 );
        assertTrue( in.getConsumedAmount() == 4 );
        assertTrue( in.getColumn() == 0 );
        assertTrue( in.getLineNumber() == 4 );
        assertEquals( new String( buf ), "\n\n\n" );
    }

    @Test
    public void testUnreadBatchMultilineAllNewlines1() throws Exception {
        implCRUnreadBatchMultilineAllNewlines( "ABC\r\n\r\n\r\n\r\n" );
    }

    @Test
    public void testUnreadBatchMultilineAllNewlines2() throws Exception {
        implCRUnreadBatchMultilineAllNewlines( "ABC\n\r\n\r\n\r\n\r" );
    }

    @Test
    public void testUnreadBatchMultilineAllNewlines3() throws Exception {
        implCRUnreadBatchMultilineAllNewlines( "ABC\n\n\n\n\n" );
    }

    @Test
    public void testUnreadBatchMultilineAllNewlines4() throws Exception {
        implCRUnreadBatchMultilineAllNewlines( "ABC\r\r\r\r\r" );
    }

    protected void implCRUnreadBatchMultilineAllNewlines( final String data ) throws Exception {
        IonCharacterReader in = createReader( data, 3 );
        char[] buf = new char[ 3 ];
        in.skip( 3L );
        int num = in.read( buf );

        assertTrue( num == 3 );
        assertTrue( in.getConsumedAmount() == 6 );
        assertTrue( in.getColumn() == 0 );
        assertTrue( in.getLineNumber() == 4 );
        assertEquals( new String( buf ), "\n\n\n" );

        in.unread( buf );
        assertTrue( in.getConsumedAmount() == 3 );
        assertTrue( in.getColumn() == 3 );
        assertTrue( in.getLineNumber() == 1 );
    }

    @Test
    public void testUnreadUnderflow() throws Exception {
        IonCharacterReader in = createReader( "ABCDE", 1 );

        char[] buf = new char[ 4 ];
        in.read( buf );
        assertEquals( new String( buf ), "ABCD" );

        try {
            in.unread( buf );
            fail();
        } catch ( IOException e ) {}
    }

    @Test
    public void testSkip() throws Exception {
        IonCharacterReader in = createReader( "\r\n\r\nABCDEFG", 4 );

        in.skip( 4 );
        assertTrue( in.getConsumedAmount() == 4 );
        assertTrue( in.getColumn() == 2 );
        assertTrue( in.getLineNumber() == 3 );
    }
}
