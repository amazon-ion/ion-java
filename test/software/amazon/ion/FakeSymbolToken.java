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

package software.amazon.ion;

import software.amazon.ion.SymbolToken;
import software.amazon.ion.UnknownSymbolException;

/**
 * NOT SUITABLE FOR PUBLIC USE since it doesn't enforce correctness.
 */
public class FakeSymbolToken
    implements SymbolToken
{
    private final String myText;
    private final int mySid;

    public FakeSymbolToken(String text, int sid)
    {
        myText = text;
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
}
