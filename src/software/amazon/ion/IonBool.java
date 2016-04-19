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



/**
 * An Ion <code>bool</code> value.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
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
