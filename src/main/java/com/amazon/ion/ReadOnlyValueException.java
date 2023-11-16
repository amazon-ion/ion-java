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
 * An error caused by an attempt to modify a read-only component.
 *
 * @see IonValue#makeReadOnly()
 */
public class ReadOnlyValueException
    extends IonException
{
    private static final long serialVersionUID = 1L;

    public ReadOnlyValueException()
    {
        super("Read-only IonValue cannot be modified");
    }

    public ReadOnlyValueException(Class type)
    {
        super("Cannot modify read-only instance of " + type);
    }
}
