// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion;

import com.amazon.ion.impl._Private_SymbolToken;

/**
 * NOT SUITABLE FOR PUBLIC USE since it doesn't enforce correctness.
 */
public class FakeSymbolToken
    implements SymbolToken, _Private_SymbolToken
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

    @Override
    public String toString()
    {
        return "SymbolToken::{text:" + myText + ",id:" + mySid + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof SymbolToken)) return false;

        SymbolToken other = (SymbolToken) o;
        if(getText() == null || other.getText() == null){
            return getText() == other.getText();
        }
        return getText().equals(other.getText());
    }

    @Override
    public int hashCode() {
        if(getText() != null) return getText().hashCode();
        return 0;
    }
}
