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

package com.amazon.ion.util;

import org.junit.Test;

public class JarInfoTest
{
    @Test
    public void testConstruction()
    {
        // This attempts to load the properties file, but within IDE its
        // not on the classpath.  At least we'll make sure the constructor
        // doesn't die when the file is missing.
        JarInfo info = new JarInfo();
    }

}