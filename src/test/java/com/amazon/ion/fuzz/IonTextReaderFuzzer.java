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

package com.amazon.ion.fuzz;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.system.IonSystemBuilder;
import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;

public class IonTextReaderFuzzer {

    @FuzzTest(maxDuration = "5m")
    public void myFuzzTest(FuzzedDataProvider data) {
        IonSystem ionSys = IonSystemBuilder.standard().build();
        String input = data.consumeRemainingAsString();
        if (input.isEmpty()) {
            return;
        }

        try (IonReader reader = ionSys.newReader(input)) {
            while (reader.next() != null) {
                // Basic traversal
            }
        } catch (Exception e) {
            // Expected for malformed text
        }
    }
}
