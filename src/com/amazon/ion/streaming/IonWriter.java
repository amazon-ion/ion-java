/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.Date;

/**
 *
 */
public interface IonWriter
{
    public abstract void setSymbolTable(UnifiedSymbolTable symbols);
    
    public abstract void writeFieldnameId(int id);
    public abstract void writeFieldname(String name);
    
    public abstract void writeAnnotations(String [] annotations);
    public abstract void writeAnnotationIds(int[] annotationIds);
    public abstract void addAnnotation(String annotation);
    public abstract void addAnnotationId(int annotationId);
    
    // writeIonContent
    public abstract void writeIonValue(IonValue value) throws IOException;
    public abstract void writeIonValue(IonType t, IonIterator iterator) throws IOException;

    public abstract void writeIonEvents(IonIterator iterator) throws IOException;

    public abstract void writeNull() throws IOException;
    public abstract void writeNull(IonType type) throws IOException;
    public abstract void writeBool(boolean value) throws IOException;
    public abstract void writeInt(byte value) throws IOException;
    public abstract void writeInt(short value) throws IOException;
    public abstract void writeInt(int value) throws IOException;
    public abstract void writeInt(long value) throws IOException;
    public abstract void writeFloat(float value) throws IOException;
    public abstract void writeFloat(double value) throws IOException;
    public abstract void writeDecimal(BigDecimal value) throws IOException;
    public abstract void writeTimestampUTC(Date value) throws IOException;
    public abstract void writeTimestamp(Date value, int localOffset) throws IOException;
    public abstract void writeSymbol(int symbolId) throws IOException;
    public abstract void writeSymbol(String value) throws IOException;
    public abstract void writeString(String value) throws IOException;
    public abstract void writeClob(byte[] value) throws IOException;
    public abstract void writeClob(byte[] value, int start, int len) throws IOException;
    public abstract void writeBlob(byte[] value) throws IOException;
    public abstract void writeBlob(byte[] value, int start, int len) throws IOException;
    
    public abstract void startStruct() throws IOException;
    public abstract void startList() throws IOException;
    public abstract void startSexp() throws IOException;
    public abstract void closeStruct() throws IOException;
    public abstract void closeList() throws IOException;
    public abstract void closeSexp() throws IOException;
    public abstract boolean isInStruct();
    
    public abstract void writeBoolList(boolean[] values) throws IOException;
    public abstract void writeIntList(byte[] values) throws IOException;
    public abstract void writeIntList(short[] values) throws IOException;
    public abstract void writeIntList(int[] values) throws IOException;
    public abstract void writeIntList(long[] values) throws IOException;
    public abstract void writeFloatList(float[] values) throws IOException;
    public abstract void writeFloatList(double[] values) throws IOException;
    public abstract void writeStringList(String[] values) throws IOException;
    
    public abstract SymbolTable getSymbolTable();
    
    public abstract byte[] getBytes() throws IOException;
    public abstract int    getBytes(byte[] bytes, int offset, int maxlen) throws IOException;
    public abstract int    writeBytes(OutputStream out) throws IOException;
}
