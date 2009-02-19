/* Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.io.IOException;



/**
 * An Ion <code>blob</code> value.
 */
public interface IonBlob
    extends IonLob
{
    /**
     * Renders the content of this blob as Base64 text.
     *
     * @param out will receive the Base64 content.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     * @throws NullPointerException if <code>out</code> is null.
     * @throws IOException if there's a problem writing to the output stream.
     *
     * @deprecated renamed to {@link #printBase64(Appendable)} for consistency.
     */
    @Deprecated
    public void appendBase64(Appendable out)
        throws NullValueException, IOException;

    /**
     * Prints the content of this blob as Base64 text, without Ion's
     * surrounding double-braces <code>{{ }}</code>.
     *
     * @param out will receive the Base64 content.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     * @throws NullPointerException if <code>out</code> is null.
     * @throws IOException if there's a problem writing to the output stream.
     */
    public void printBase64(Appendable out)
        throws NullValueException, IOException;


    public IonBlob clone();
}
