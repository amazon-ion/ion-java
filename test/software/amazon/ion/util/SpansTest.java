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

package software.amazon.ion.util;

import static org.junit.Assert.assertNull;
import static software.amazon.ion.util.Spans.currentSpan;

import org.junit.Test;
import software.amazon.ion.Span;
import software.amazon.ion.TextSpan;

public class SpansTest
{
    @Test
    public void testCurrentSpan()
    {
        Span s = currentSpan(null);
        assertNull("span should be null", s);

        s = currentSpan(new Object());
        assertNull("span should be null", s);
    }


    @Test
    public void testCurrentSpanFacet()
    {
        TextSpan s = currentSpan(TextSpan.class, null);
        assertNull("span should be null", s);

        s = currentSpan(TextSpan.class, new Object());
        assertNull("span should be null", s);
    }
}
