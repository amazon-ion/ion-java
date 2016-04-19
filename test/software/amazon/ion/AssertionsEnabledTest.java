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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class AssertionsEnabledTest
{
    /**
     * Make sure our unit test framework has assertions enabled.
     */
    @Test
    public void testAssertionsEnabled()
    {
        String message = "Java assertion failure";
        try
        {
            assert false : message;
            fail("Assertions not enabled");
        }
        catch (AssertionError e)
        {
            assertEquals(message, e.getMessage());
        }
    }
}
