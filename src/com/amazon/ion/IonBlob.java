/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved. */

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
     */
    public void appendBase64(Appendable out)
        throws NullValueException, IOException;


    public IonBlob clone();
}
