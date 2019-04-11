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
 * An error caused by use of an invalid symbol starting with
 * <code>"$ion_"</code>.
 */
public class InvalidSystemSymbolException
    extends IonException
{
    private static final long serialVersionUID = 2206499395645594047L;

    private String myBadSymbol;


    public InvalidSystemSymbolException(String badSymbol)
    {
        super("Invalid system symbol '" + badSymbol + "'");
        myBadSymbol = badSymbol;
    }


    public String getBadSymbol()
    {
        return myBadSymbol;
    }
}
