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

import com.amazon.ion.Timestamp;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.matchesPattern;

public class JarInfoTest {
    @Test
    public void getBuildTime() {
        JarInfo info = new JarInfo();
        Timestamp timestamp = info.getBuildTime();
        // This test was written at the timestamp below, the loaded time should always be greater
        Timestamp original = Timestamp.valueOf("2022-05-10T22:56:48Z");
        assertThat(timestamp, greaterThan(original));
    }

    @Test
    public void getProjectVersion() {
        JarInfo info = new JarInfo();
        String projectVersion = info.getProjectVersion();
        // Semantic version MAJOR.MINOR.PATCH with optional -SNAPSHOT suffix
        // Should always match our project version
        String projectVersionPattern = "\\d+\\.\\d+\\.\\d+(-SNAPSHOT)?";
        assertThat(projectVersion, matchesPattern(projectVersionPattern));
    }

}