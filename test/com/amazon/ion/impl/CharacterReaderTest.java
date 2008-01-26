/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonTestCase;
import java.io.IOException;
import java.io.StringReader;

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
    
    public void testEmpty() throws Exception {
        IonCharacterReader in = createReader( "", 1 );
        assertTrue( in.read() == -1 );
        assertTrue( in.getConsumedAmount() == 0 );
        assertTrue( in.getColumn() == 0 );
        assertTrue( in.getLineNumber() == 1 );
    }
    
    public void testEmptyMultipleRead() throws Exception {
        IonCharacterReader in = createReader( "", 1 );
        assertTrue( in.read() == -1 );
        assertTrue( in.read() == -1 );
    }
    
    public void testEmptyUnreadEOF() throws Exception {
        IonCharacterReader in = createReader( "", 1 );
        assertTrue( in.read() == -1 );
        in.unread( -1 );
        assertTrue( in.read() == -1 );
    }
    
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
    
    public void testSingleUnreadCarriageReturn() throws Exception {
        IonCharacterReader in = createReader( "B", 1 );
        
        try {
            in.unread( '\r' );
            fail();
        } catch ( final IOException e ) {}
    }
    
    public void testEmptyUnread() throws Exception {
        IonCharacterReader in = createReader( "", 1 );
        
        assertTrue( in.read() == -1 );
        in.unread( 'A' );
        assertTrue( in.read() == 'A' );
        assertTrue( in.read() == -1 );
    }
    
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
    
    public void testUnreadBatchMultiline() throws Exception {
        IonCharacterReader in = createReader( "A\r\n\n\nB\n\nC\n\nD", 3 );
        
        in.read();
        char[] buf = new char[ 3 ];
        int num = in.read( buf );
        
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
    
    public void testUnreadBatchMultilineAllNewlines1() throws Exception {
        implCRUnreadBatchMultilineAllNewlines( "ABC\r\n\r\n\r\n\r\n" );
    }
    
    public void testUnreadBatchMultilineAllNewlines2() throws Exception {
        implCRUnreadBatchMultilineAllNewlines( "ABC\n\r\n\r\n\r\n\r" );
    }
    
    public void testUnreadBatchMultilineAllNewlines3() throws Exception {
        implCRUnreadBatchMultilineAllNewlines( "ABC\n\n\n\n\n" );
    }
    
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
    
    public void testSkip() throws Exception {
        IonCharacterReader in = createReader( "\r\n\r\nABCDEFG", 4 );
        
        in.skip( 4 );
        assertTrue( in.getConsumedAmount() == 4 );
        assertTrue( in.getColumn() == 2 );
        assertTrue( in.getLineNumber() == 3 );
    }
}
