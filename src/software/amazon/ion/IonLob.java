/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion;

import java.io.InputStream;

/**
 * Common functionality of Ion <code>blob</code> and <code>clob</code>
 * types.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonLob
    extends IonValue
{
    /**
     * Creates a new {@link InputStream} that returns the data as raw
     * bytes.
     *
     * @return a new stream positioned at the start of the lob,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     */
    public InputStream newInputStream();

    /**
     * Gets all the data of this lob, or <code>null</code> if this is an Ion
     * <code>null</code> value.
     *
     * @return a new byte array,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     */
    public byte[] getBytes();

    /**
     * Sets the data of this lob, copying bytes from an array.
     *
     * @param bytes the new data for the lob;
     * may be <code>null</code> to make this an Ion <code>null</code> value.
     */
    public void setBytes(byte[] bytes);

    /**
     * Sets the data of this lob, copying bytes from part of an array.
     * <p>
     * This method copies {@code length} bytes from the given array into this
     * value, starting at the given offset in the array.
     *
     * @param bytes the new data for the lob;
     * may be <code>null</code> to make this an Ion <code>null</code> value.
     * @param offset the offset within the array of the first byte to copy;
     * must be non-negative and no larger than {@code bytes.length}.
     * @param length the number of bytes to be copied from the given array;
     * must be non-negative and no larger than {@code bytes.length - offset}.
     *
     * @throws IndexOutOfBoundsException
     * if the preconditions on the {@code offset} and {@code length} parameters
     * are not met.
     */
    public void setBytes(byte[] bytes, int offset, int length);

    /**
     * Gets the size in bytes of this lob.
     *
     * @return the lob's size in bytes.
     */
    public int byteSize();

    public IonLob clone()
        throws UnknownSymbolException;
}
