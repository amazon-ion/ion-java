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

import static software.amazon.ion.ReaderMaker.valuesExcluding;
import static software.amazon.ion.util.Spans.currentSpan;

import org.junit.Test;
import software.amazon.ion.ReaderMaker;
import software.amazon.ion.Span;
import software.amazon.ion.TextSpan;
import software.amazon.ion.facet.Facets;
import software.amazon.ion.junit.Injected.Inject;

/**
 * @see NonTextSpanTest
 */
public class TextSpanTest
    extends ReaderFacetTestCase
{
    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS =
        valuesExcluding(NON_TEXT_SPAN_READERS);


    public TextSpanTest()
    {
        mySeekableReaderRequired = false;
    }


    protected void expectNextSpan(int startLine, int startColumn)
    {
        in.next();

        Span s = sp.currentSpan();
        TextSpan ts = Facets.assumeFacet(TextSpan.class, s);
        checkSpan(startLine, startColumn, ts);

        ts = currentSpan(TextSpan.class, in);
        checkSpan(startLine, startColumn, ts);
    }


    @Test
    public void testTrivialSpan()
    {
        read("1 true\n 'hallo'");  // TODO test all types

        expectNextSpan(1, 1);
        expectNextSpan(1, 3);
        expectNextSpan(2, 2);
    }
}
