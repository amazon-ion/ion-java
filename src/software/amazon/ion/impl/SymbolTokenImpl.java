/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl;

import static software.amazon.ion.util.IonTextUtils.printString;

import software.amazon.ion.SymbolToken;
import software.amazon.ion.UnknownSymbolException;


final class SymbolTokenImpl
    implements SymbolToken
{
    private final String myText;
    private final int mySid;

    SymbolTokenImpl(String text, int sid)
    {
        assert text != null || sid > 0 : "Neither text nor sid is defined";

        myText = text;
        mySid = sid;
    }

    SymbolTokenImpl(int sid)
    {
        assert sid > 0 : "sid is undefined";

        myText = null;
        mySid = sid;
    }


    public String getText()
    {
        return myText;
    }

    public String assumeText()
    {
        if (myText == null) throw new UnknownSymbolException(mySid);
        return myText;
    }

    public int getSid()
    {
        return mySid;
    }

    @Override
    public String toString()
    {
        String text = (myText == null ? null : printString(myText));
        return "SymbolToken::{text:" + text + ",id:" + mySid + "}";
    }
}
