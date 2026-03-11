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
