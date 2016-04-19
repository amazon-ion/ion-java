/*
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.util;

import java.io.IOException;

/**
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public interface PrivateFastAppendable
    extends Appendable
{
    /**
     * High performance method for appending an ASCII character. METHOD DOESN'T
     * VERIFY IF CHARACTER IS ASCII.
     * @param c
     * @throws IOException
     */
    public void appendAscii(char c)
        throws IOException;

    /**
     * High performance method for appending a sequence of ASCII characters.
     * METHOD DOESN'T VERIFY IF CHARACTERS ARE ASCII.
     * @param csq
     * @throws IOException
     */
    public void appendAscii(CharSequence csq)
        throws IOException;

    /**
     * High performance method for appending a range in sequence of ASCII
     * characters. METHOD DOESN'T VERIFY IF CHARACTERS ARE ASCII.
     * @param csq
     * @param start
     * @param end
     * @throws IOException
     */
    public void appendAscii(CharSequence csq, int start, int end)
        throws IOException;

    /**
     * High performance method for appending a UTF-16 non-surrogate character.
     * METHOD DOESN'T VERIFY IF CHARACTER IS OR IS NOT SURROGATE.
     * @param c
     * @throws IOException
     */
    public void appendUtf16(char c)
        throws IOException;

    /**
     * High performance method for appending a UTF-16 surrogate pair. METHOD
     * DOESN'T VERIFY IF LEAD AND TRAIL SURROGATES ARE VALID.
     * @param leadSurrogate
     * @param trailSurrogate
     * @throws IOException
     */
    public void appendUtf16Surrogate(char leadSurrogate, char trailSurrogate)
        throws IOException;
}
