// Copyright (c) 2013-2014 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion;

import java.io.IOException;


public interface FastAppendable
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
