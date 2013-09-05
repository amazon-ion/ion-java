/* Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.io.IOException;



/**
 * An Ion <code>blob</code> value.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonBlob
    extends IonLob
{
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


    public IonBlob clone()
        throws UnknownSymbolException;
}
