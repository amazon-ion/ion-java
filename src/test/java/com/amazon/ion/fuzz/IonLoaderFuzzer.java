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
