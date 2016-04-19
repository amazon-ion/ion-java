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

package software.amazon.ion.streaming;

import org.junit.Test;
import software.amazon.ion.ReaderMaker;
import software.amazon.ion.Span;
import software.amazon.ion.TextSpan;
import software.amazon.ion.junit.Injected.Inject;

/**
 * @see TextSpanTest
 */
public class NonTextSpanTest
    extends ReaderFacetTestCase
{
    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS = NON_TEXT_SPAN_READERS;


    public NonTextSpanTest()
    {
        mySpanProviderRequired = false;
        mySeekableReaderRequired = false;
    }


    @Test
    public void noTextSpanFacet()
    {
        read("null");
        in.next();
        if (sp != null)
        {
            Span s = sp.currentSpan();
            expectNoFacet(TextSpan.class, s);
        }
    }
}
