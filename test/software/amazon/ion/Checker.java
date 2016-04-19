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

public interface Checker
{
    /**
     * @param expectedText null means text isn't known.
     */
    Checker fieldName(String expectedText, int expectedSid);

    /** Check the first annotation's text */
    Checker annotation(String expectedText);

    /** Check the first annotation's text and sid */
    Checker annotation(String expectedText, int expectedSid);

    /** Check that all the annotations exist in the given order. */
    Checker annotations(String[] expectedTexts, int[] expectedSids);
}
