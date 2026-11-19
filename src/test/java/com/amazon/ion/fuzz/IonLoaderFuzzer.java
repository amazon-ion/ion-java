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

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;
import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;

public class IonLoaderFuzzer {

    @FuzzTest(maxDuration = "5m")
    public void myFuzzTest(FuzzedDataProvider data) {
        IonSystem ionSys = IonSystemBuilder.standard().build();
        IonLoader loader = ionSys.getLoader();
        byte[] input = data.consumeRemainingAsBytes();
        if (input.length == 0) {
            return;
        }

        try {
            // Loader.load(byte[]) auto-detects text vs binary and handles GZIP
            IonDatagram datagram = loader.load(input);
            // Traverse the datagram to hit more value-specific paths
            datagram.size();
            datagram.iterator().forEachRemaining(val -> {
                val.getType();
                val.isNullValue();
            });
        } catch (Exception e) {
            // Expected for malformed data
        }
    }
}
