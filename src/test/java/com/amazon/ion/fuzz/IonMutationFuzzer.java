package com.amazon.ion.fuzz;

import com.amazon.ion.*;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.util.Equivalence;
import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;

public class IonMutationFuzzer {

    @FuzzTest(maxDuration = "10m")
    public void myFuzzTest(FuzzedDataProvider data) {
        IonSystem ionSys = IonSystemBuilder.standard().build();
        byte[] input = data.consumeBytes(data.consumeInt(0, 1000));
        
        IonDatagram datagram;
        try {
            datagram = ionSys.getLoader().load(input);
        } catch (Exception e) {
            return;
        }

        if (datagram.size() == 0) return;

        // Perform some mutations
        int mutations = data.consumeInt(1, 20);
        for (int i = 0; i < mutations; i++) {
            if (datagram.size() == 0) break;
            
            int index = data.consumeInt(0, datagram.size() - 1);
            IonValue val = datagram.get(index);
            
            int action = data.consumeInt(0, 5);
            try {
                switch (action) {
                    case 0: // Add annotation
                        val.addTypeAnnotation(data.consumeString(10));
                        break;
                    case 1: // Clear annotations
                        val.clearTypeAnnotations();
                        break;
                    case 2: // Clone and check equivalence
                        IonValue clone = val.clone();
                        if (!Equivalence.ionEquals(val, clone)) {
                            throw new RuntimeException("Clone not equal to original");
                        }
                        break;
                    case 3: // ToString check
                        val.toString();
                        val.toPrettyString();
                        break;
                    case 4: // HashCode check
                        val.hashCode();
                        break;
                    case 5: // Remove from container
                        val.removeFromContainer();
                        break;
                }
            } catch (Exception e) {
                // Ignore mutation errors
            }
        }
    }
}
