/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

/**
 * An Ion <code>string</code> value.
 */
public interface IonString
    extends IonText
{
    /**
     * Gets the characters of this string.
     *
     * @return the text of the string, or <code>null</code> if this is
     * <code>null.string</code>.
     */
    public String stringValue();

    /**
     * Changes the value of this string.
     *
     * @param value the new value of this string;
     * may be <code>null</code> to make this <code>null.string</code>.
     */
    public void setValue(String value);


    public IonString clone();
}
