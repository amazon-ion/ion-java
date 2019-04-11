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
 * An error caused by a symbol ID that could not be translated into text
 * because it is not defined by the symbol table in context.
 * <p>
 * When this occurs, it's likely that the {@link IonCatalog} in effect does
 * not have the relevant shared symbol tables needed to decode Ion binary
 * data.
 *
 */
public class UnknownSymbolException
    extends IonException
{
    private static final long serialVersionUID = 1L;

    private final int mySid;
    private final String myText;

    public UnknownSymbolException(int sid)
    {
        mySid = sid;
        myText = null;
    }
    public UnknownSymbolException(String message)
    {
        myText = message;
        mySid = 0;
    }

    public int getSid()
    {
        return mySid;
    }

    @Override
    public String getMessage()
    {
        if(myText == null) {
            return "Unknown symbol text for $" + mySid;
        } else {
            return myText;
        }
    }
}
