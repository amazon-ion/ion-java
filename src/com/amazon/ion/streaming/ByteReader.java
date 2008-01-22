/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import com.amazon.ion.impl.IonTokenReader;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * Interface to read bytes over a variety of sources.
 * Avoiding the Java standard overhead in so far as possible.
 * This requires the ability to access any part of the underlying
 * data source randomly.  However forward access is expected to be
 * the standard direction.
 */
public interface ByteReader
{
    public static final int EOF = -1;
    
    public int  position();
    public void position(int newPosition);
    public void skip(int length);
    
    public int  read() throws IOException;
    public int  read(byte[] dst, int start, int len) throws IOException;
    
    public int  readTypeDesc() throws IOException;
    
    public long readLong(int len) throws IOException;
    public long readVarLong() throws IOException;
    public long readVarULong() throws IOException;
    public long readULong(int len) throws IOException;

    public int  readInt(int len) throws IOException;
    public int  readVarInt() throws IOException;
    public int  readVarUInt() throws IOException;

    public double readFloat(int len) throws IOException;
    public BigDecimal readDecimal(int len) throws IOException;
    public IonTokenReader.Type.timeinfo readTimestamp(int len) throws IOException;
    public String readString(int len) throws IOException;
}
