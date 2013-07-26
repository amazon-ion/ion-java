/* Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;



/**
 * An Ion <code>bool</code> value.
 */
public interface IonBool
    extends IonValue
{
    /**
     * Gets the value of this Ion <code>bool</code> as a Java
     * <code>boolean</code> value.
     *
     * @return the boolean value.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public boolean booleanValue()
        throws NullValueException;

    /**
     * Sets this instance to have a specific value.
     *
     * @param b the new value for this <code>bool</code>.
     */
    public void setValue(boolean b);

    /**
     * Sets this instance to have a specific value.
     *
     * @param b the new value for this <code>bool</code>;
     * may be <code>null</code> to make this <code>null.bool</code>.
     */
    public void setValue(Boolean b);

    public IonBool clone()
        throws UnknownSymbolException;
}
