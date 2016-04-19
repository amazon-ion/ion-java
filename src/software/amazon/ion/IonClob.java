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

import java.io.Reader;
import java.nio.charset.Charset;

/**
 * An Ion <code>clob</code> value.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonClob
    extends IonValue, IonLob
{
    /**
     * Creates a new {@link Reader} that provides the value of this clob as
     * text, decoding the raw bytes using a given character set.
     *
     * @param cs must not be <code>null</code>.
     * @return a new reader positioned at the start of the clob,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     */
    public Reader newReader(Charset cs);


    /**
     * Gets the value of this clob as a Java {@link String} value, decoding
     * the raw bytes using a given character set.  This is a convenience
     * wrapper around {@link #newReader(Charset)}.
     * <p>
     * The behavior of this method when the clob bytes are not valid in the
     * given charset is unspecified.
     *
     * @param cs must not be <code>null</code>.
     * @return the decoded text,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     */
    public String stringValue(Charset cs);


    public IonClob clone()
        throws UnknownSymbolException;
}
