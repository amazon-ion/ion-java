
/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.ion.impl;

import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.util._Private_FastAppendable;
import java.io.IOException;
import org.junit.Assert;

public class TestMarkupCallback
    extends _Private_MarkupCallback
{
    public static class Builder
        implements _Private_CallbackBuilder
    {
        public _Private_MarkupCallback build(_Private_FastAppendable rawOutput)
        {
            return new TestMarkupCallback(rawOutput);
        }
    }

    private final _Private_FastAppendable myAppendable;

    public TestMarkupCallback(_Private_FastAppendable out) {
        super(out);
        myAppendable = out;
    }

    public void checkForNulls(IonType iType) {
        Assert.assertNotNull("iType is null", iType);
        Assert.assertNotNull("myOut is null", myAppendable);
    }

    @Override
    public void beforeValue(IonType iType)
        throws IOException
    {
        checkForNulls(iType);
        myAppendable.append("<beforeData " + iType + ">");
    }

    @Override
    public void afterValue(IonType iType)
        throws IOException
    {
        checkForNulls(iType);
        myAppendable.append("<afterData " + iType + ">");
    }

    @Override
    public void afterStepIn(IonType iType)
        throws IOException
    {
        checkForNulls(iType);
        myAppendable.append("<afterStepIn " + iType + ">");
    }

    @Override
    public void beforeStepOut(IonType iType)
        throws IOException
    {
        checkForNulls(iType);
        myAppendable.append("<beforeStepOut " + iType + ">");
    }

    @Override
    public void beforeFieldName(IonType iType, SymbolToken name)
        throws IOException
    {
        checkForNulls(iType);
        Assert.assertNotNull("beforeFieldName: name is null", name);
        myAppendable.append("<beforeFieldName " + iType + ">");
    }

    @Override
    public void afterFieldName(IonType iType, SymbolToken name)
        throws IOException
    {
        checkForNulls(iType);
        Assert.assertNotNull("afterFieldName: name is null", name);
        myAppendable.append("<afterFieldName " + iType + ">");
    }

    @Override
    public void beforeSeparator(IonType iType)
        throws IOException
    {
        checkForNulls(iType);
        myAppendable.append("<beforeSeparator " + iType + ">");
    }

    @Override
    public void afterSeparator(IonType iType)
        throws IOException
    {
        checkForNulls(iType);
        myAppendable.append("<afterSeparator " + iType + ">");
    }

    @Override
    public void beforeAnnotations(IonType iType)
        throws IOException
    {
        checkForNulls(iType);
        myAppendable.append("<beforeAnnotations " + iType + ">");
    }

    @Override
    public void afterAnnotations(IonType iType)
        throws IOException
    {
        checkForNulls(iType);
        myAppendable.append("<afterAnnotations " + iType + ">");
    }

    @Override
    public void beforeEachAnnotation(IonType iType, SymbolToken ann)
        throws IOException
    {
        checkForNulls(iType);
        Assert.assertNotNull("beforeEachAnnotation: ann is null", ann);
        myAppendable.append("<beforeEachAnnotation " + iType + ">");
    }

    @Override
    public void afterEachAnnotation(IonType iType, SymbolToken ann)
        throws IOException
    {
        checkForNulls(iType);
        Assert.assertNotNull("afterEachAnnotation: ann is null", ann);
        myAppendable.append("<afterEachAnnotation " + iType + ">");
    }
}
