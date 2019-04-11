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

import com.amazon.ion.Decimal;
import com.amazon.ion.Timestamp;
import java.io.IOException;
/**
 * Interface to read bytes over a variety of sources.
 * Avoiding the Java standard overhead in so far as possible.
 * This requires the ability to access any part of the underlying
 * data source randomly.  However forward access is expected to be
 * the standard direction.
 */
interface ByteReader
{
    /**
     * yet another definition of eof of file as -1
     */
    public static final int EOF = -1;

    /**
     * returns the current position of the reader in the underlying
     * byte buffer
     * @return offset of the next byte to read
     */
    public int  position();
    /**
     * sets the position of the reader so that it will read the
     * byte at the passed in offset (newPosition) on the next call
     * to read()
     * @param newPosition offset to position the reader to
     */
    public void position(int newPosition);
    /**
     * moves the reader forward length bytes.  This is typically
     * faster than read as it has no need to actually move any
     * bytes of data.  It may error if the skip would take the
     * current position off the end of the buffer.
     * @param length number of bytes to skip
     */
    public void skip(int length);

    /**
     * reads a single byte and returns it as an unsigned value,
     * that is from 0 to 255.  This returns a ByteReader.EOF (-1)
     * if there is not data to read.
     * @return next byte of data
     * @throws IOException
     */
    public int  read() throws IOException;
    /**
     * reads a number of bytes into the callers supplied buffer (dst).
     * This will return the number of bytes read, if any.
     * @param dst callers destination byte array
     * @param start offset of the first destination array element to write into
     * @param len maximum number of bytes to copy
     * @return number of bytes actually copied
     * @throws IOException
     */
    public int  read(byte[] dst, int start, int len) throws IOException;

    /**
     * reads the next byte as a typedesc bytes
     * @return typedesc byte
     * @throws IOException
     */
    public int  readTypeDesc() throws IOException;

    /**
     * reads the upcoming bytes as a signed long.  This uses
     * 7 bits per byte, expects the bytes in bigendian order, and
     * looks for the high order bit (0x80) to be set to mark the
     * end of the value. It expects the 2nd bit (0x40 of the first
     * byte) to be the sign of the value.  The remaining bits are
     * the absolute value of the orginal value. It
     * returns the aggregated bytes as a Java long (signed 64 bit
     * value) correctly signed.
     * @return long value read
     * @throws IOException
     */
    public long readVarLong() throws IOException;
    /**
     * reads the upcoming bytes as a unsigned long.  This uses
     * 7 bits per byte, expects the bytes in bigendian order, and
     * looks for the high order bit (0x80) to be set to mark the
     * end of the value. It does not include a sign bit so all 7 bits
     * in the first byte are treated as positive. It
     * returns the aggregated bytes as a Java long (signed 64 bit
     * value).
     * @return long value read
     * @throws IOException
     */
    public long readVarULong() throws IOException;
    /**
     * reads the next len bytes as a signed long.  This uses
     * 8 bits per byte and expects the bytes in bigendian order. It
     * returns the aggregated bytes as a Java long (signed 64 bit
     * value).
     * @param len number of bytes to read into the long
     * @return long value read
     * @throws IOException
     */
    public long readULong(int len) throws IOException;

    /**
     * reads the upcoming bytes as a signed int.  This uses
     * 7 bits per byte, expects the bytes in bigendian order, and
     * looks for the high order bit (0x80) to be set to mark the
     * end of the value. It expects the 2nd bit (0x40 of the first
     * byte) to be the sign of the value.  The remaining bits are
     * the absolute value of the orginal value. It
     * returns the aggregated bytes as a Java long (signed 32 bit
     * value) correctly signed.
     * @return int value read
     * @throws IOException
     */
    public int  readVarInt() throws IOException;
    /**
     * reads the upcoming bytes as a unsigned int.  This uses
     * 7 bits per byte, expects the bytes in bigendian order, and
     * looks for the high order bit (0x80) to be set to mark the
     * end of the value. It does not include a sign bit so all 7 bits
     * in the first byte are treated as positive. It
     * returns the aggregated bytes as a Java long (signed 32 bit
     * value).
     * @return int value read
     * @throws IOException
     */
    public int  readVarUInt() throws IOException;

    /**
     * reads len bytes and treats them as an IEEE 64 binary
     * floating point value.  Currently the only valid values for
     * len are 0 (which returns 0.0e1) or 8 which is treated
     * as a 64 bit binary float and returned.
     * @param len 0 or 8
     * @return 0.0 or the double represented by the next 8 bytes
     * @throws IOException
     */
    public double readFloat(int len) throws IOException;
    /**
     * reads the next len bytes and converts them to a {@link Decimal} value.
     * @param len number of bytes over which the value extends
     * @return Decimal object initialized to the represented value; not null.
     * @throws IOException
     */
    public Decimal readDecimal(int len) throws IOException;

    /**
     * Reads the next len bytes and returns them as a {@link Timestamp}
     * object.  This contains an UTC date and time value and a
     * timezone offset of the original value.
     *
     * @param len
     * @return a new {@link Timestamp} instance,
     * or {@code null} if the encoded value is {@code null.timestamp}.
     * @throws IOException
     */
    public Timestamp readTimestamp(int len) throws IOException;

    /** reads the next len bytes and converts them from UTF-8 into Java
     * characters and returns those characters as a String.  This may throw
     * an exception in the event it encounters byte sequences that are not
     * value UTF-8.  This converts some Unicode values into character pairs
     * when the value requires the use of surrogate pairs in Unicode 16.
     * @param len number of bytes to read
     * @return resultant Java string
     * @throws IOException
     */
    public String readString(int len) throws IOException;
}
