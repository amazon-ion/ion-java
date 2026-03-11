package com.amazon.ion.fuzz;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonSystemBuilder;
import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;

import java.io.ByteArrayOutputStream;

public class IonRoundTripFuzzer {

    @FuzzTest(maxDuration = "10m")
    public void myFuzzTest(FuzzedDataProvider data) {
        IonSystem ionSys = IonSystemBuilder.standard().build();
        byte[] input = data.consumeRemainingAsBytes();
        if (input.length == 0) {
            return;
        }

        try (IonReader reader = ionSys.newReader(input)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (IonWriter writer = ionSys.newBinaryWriter(baos)) {
                writer.writeValues(reader);
            }
            // We could also take the output and read it back, but writeValues already
            // exercises both the reader (to get values) and the writer (to put values).
        } catch (Exception e) {
            // Expected exceptions for malformed input
        }
    }
}
