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
import java.math.BigDecimal;

/**
 * Interface to write Ion value to bytes over a variety of destinations.
 * Avoiding the Java standard overhead in so far as possible.
 * This requires the ability to access any part of the underlying
 * data source randomly and insert space at any position.  However forward
 * access is expected to be the standard direction.
 */
interface ByteWriter
{
        public void write(byte b) throws IOException;
        public void write(byte[] dst, int start, int len) throws IOException;
        public int  position();
        public void position(int newPosition);

        public void insert(int length);
        public void remove(int length);

        public void writeTypeDesc(int typeDescByte) throws IOException;
        public int  writeTypeDescWithLength(int typeid, int lenOfLength, int valueLength) throws IOException;
        public int  writeTypeDescWithLength(int typeid, int valueLength) throws IOException;

        public int  writeIonInt(long value, int len) throws IOException;
        public int  writeVarInt(long value, int len, boolean force_zero_write) throws IOException;
        public int  writeVarUInt(long value, int len, boolean force_zero_write) throws IOException;

        public int  writeIonInt(int value, int len) throws IOException;
        public int  writeVarInt(int value, int len, boolean force_zero_write) throws IOException;
        public int  writeVarUInt(int value, int len, boolean force_zero_write) throws IOException;

        public int  writeFloat(double value) throws IOException;
        public int  writeDecimal(BigDecimal value) throws IOException;
        public int  writeString(String value) throws IOException;
}
