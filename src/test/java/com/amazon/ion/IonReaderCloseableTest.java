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

package com.amazon.ion;

import com.amazon.ion.system.IonReaderBuilder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;

/**
 * Tests that IonReader properly implements Closeable and can be used with try-with-resources.
 */
public class IonReaderCloseableTest {

    @Test
    public void testIonReaderIsCloseable() {
        // Verify that IonReader extends Closeable
        assertTrue("IonReader should extend Closeable", 
                   java.io.Closeable.class.isAssignableFrom(IonReader.class));
    }

    @Test
    public void testIonReaderIsAutoCloseable() {
        // Verify that IonReader extends AutoCloseable (through Closeable)
        assertTrue("IonReader should extend AutoCloseable", 
                   AutoCloseable.class.isAssignableFrom(IonReader.class));
    }

    @Test
    public void testTryWithResourcesWithByteArray() throws IOException {
        byte[] ionData = "hello".getBytes();
        
        // Test that IonReader can be used in try-with-resources
        try (IonReader reader = IonReaderBuilder.standard().build(ionData)) {
            assertNotNull("Reader should not be null", reader);
            // Use the reader
            IonType type = reader.next();
            assertEquals("Should read a symbol", IonType.SYMBOL, type);
            assertEquals("Should read 'hello'", "hello", reader.stringValue());
        } // Reader should be automatically closed here
        
        // Test passes if no exception is thrown
    }

    @Test
    public void testTryWithResourcesWithInputStream() throws IOException {
        byte[] ionData = "world".getBytes();
        InputStream inputStream = new ByteArrayInputStream(ionData);
        
        // Test that IonReader can be used in try-with-resources with InputStream
        try (IonReader reader = IonReaderBuilder.standard().build(inputStream)) {
            assertNotNull("Reader should not be null", reader);
            // Use the reader
            IonType type = reader.next();
            assertEquals("Should read a symbol", IonType.SYMBOL, type);
            assertEquals("Should read 'world'", "world", reader.stringValue());
        } // Reader should be automatically closed here
        
        // Test passes if no exception is thrown
    }

    @Test
    public void testManualClose() throws IOException {
        byte[] ionData = "test".getBytes();
        IonReader reader = IonReaderBuilder.standard().build(ionData);
        
        try {
            assertNotNull("Reader should not be null", reader);
            // Use the reader
            IonType type = reader.next();
            assertEquals("Should read a symbol", IonType.SYMBOL, type);
            assertEquals("Should read 'test'", "test", reader.stringValue());
        } finally {
            // Manually close the reader
            reader.close();
        }
        
        // Test passes if no exception is thrown
    }

    @Test
    public void testCloseWithInputStream() throws IOException {
        byte[] ionData = "closetest".getBytes();
        
        // Create a custom InputStream that tracks if it was closed
        class TrackingInputStream extends ByteArrayInputStream {
            private boolean closed = false;
            
            public TrackingInputStream(byte[] buf) {
                super(buf);
            }
            
            @Override
            public void close() throws IOException {
                super.close();
                closed = true;
            }
            
            public boolean isClosed() {
                return closed;
            }
        }
        
        TrackingInputStream trackingStream = new TrackingInputStream(ionData);
        
        try (IonReader reader = IonReaderBuilder.standard().build(trackingStream)) {
            // Use the reader
            reader.next();
        } // Reader and underlying stream should be closed here
        
        assertTrue("Underlying InputStream should be closed", trackingStream.isClosed());
    }
}
