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

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonSystemBuilder;
import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

public class IonWriterFuzzer {

    @FuzzTest(maxDuration = "5m")
    public void myFuzzTest(FuzzedDataProvider data) {
        IonSystem ionSys = IonSystemBuilder.standard().build();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        try (IonWriter writer = data.consumeBoolean() ? ionSys.newBinaryWriter(baos) : ionSys.newTextWriter(baos)) {
            int steps = data.consumeInt(1, 50);
            for (int i = 0; i < steps; i++) {
                int action = data.consumeInt(0, 12);
                switch (action) {
                    case 0: writer.writeNull(); break;
                    case 1: writer.writeBool(data.consumeBoolean()); break;
                    case 2: writer.writeInt(data.consumeLong()); break;
                    case 3: writer.writeInt(new BigInteger(data.consumeBytes(data.consumeInt(0, 32)))); break;
                    case 4: writer.writeFloat(data.consumeDouble()); break;
                    case 5: writer.writeDecimal(new BigDecimal(data.consumeLong())); break;
                    case 6: writer.writeTimestamp(Timestamp.forMillis(data.consumeLong(), data.consumeInt(0, 100))); break;
                    case 7: writer.writeSymbol(data.consumeString(10)); break;
                    case 8: writer.writeString(data.consumeString(100)); break;
                    case 9: writer.writeClob(data.consumeBytes(data.consumeInt(0, 100))); break;
                    case 10: writer.writeBlob(data.consumeBytes(data.consumeInt(0, 100))); break;
                    case 11: writer.stepIn(com.amazon.ion.IonType.LIST); break;
                    case 12: try { writer.stepOut(); } catch (Exception e) {} break;
                }
            }
        } catch (Exception e) {
            // Expected
        }
    }
}
