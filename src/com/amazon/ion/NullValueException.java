/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion;


/**
 * An error caused by invoking an inappropriate method on an Ion
 * <code>null</code> value.
 */
public class NullValueException
    extends IonException
{
    private static final long serialVersionUID = 1L;


    public NullValueException()
    {
        super();
    }

    /**
     * @param message
     * @param cause
     */
    public NullValueException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * @param message
     */
    public NullValueException(String message)
    {
        super(message);
    }

    /**
     * @param cause
     */
    public NullValueException(Throwable cause)
    {
        super(cause);
    }
}
