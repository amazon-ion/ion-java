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

import java.io.IOException;

public class IonReaderFuzzer {

    @FuzzTest(maxDuration = "10m")
    public void myFuzzTest(FuzzedDataProvider data) {
        IonSystem ionSys = IonSystemBuilder.standard().build();
        byte[] input = data.consumeRemainingAsBytes();
        if (input.length == 0) {
            return;
        }

        try (IonReader reader = ionSys.newReader(input)) {
            processReader(reader, 0);
        } catch (Exception e) {
            // It's expected to throw various exceptions when parsing malformed data
        }
    }

    private void processReader(IonReader reader, int depth) throws Exception {
        if (depth > 20) {
            // Prevent StackOverflowError in deeply nested structures
            return;
        }

        IonType type;
        while ((type = reader.next()) != null) {
            
            // Fuzz field names and annotations if available
            try {
                reader.getFieldName();
            } catch (Exception e) { }
            
            try {
                reader.getTypeAnnotations();
            } catch (Exception e) { }

            if (reader.isNullValue()) {
                continue;
            }

            switch (type) {
                case BOOL:
                    reader.booleanValue();
                    break;
                case INT:
                    try {
                        reader.intValue();
                    } catch (Exception e) {
                        try {
                            reader.longValue();
                        } catch (Exception ex) {
                            reader.bigIntegerValue();
                        }
                    }
                    break;
                case FLOAT:
                    reader.doubleValue();
                    break;
                case DECIMAL:
                    reader.decimalValue();
                    break;
                case TIMESTAMP:
                    reader.timestampValue();
                    break;
                case SYMBOL:
                    reader.symbolValue();
                    reader.stringValue();
                    break;
                case STRING:
                    reader.stringValue();
                    break;
                case CLOB:
                case BLOB:
                    reader.newBytes();
                    reader.byteSize();
                    break;
                case LIST:
                case SEXP:
                case STRUCT:
                    reader.stepIn();
                    processReader(reader, depth + 1);
                    reader.stepOut();
                    break;
            }
        }
    }
}
