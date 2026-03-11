package com.amazon.ion.fuzz;

import com.amazon.ion.*;
import com.amazon.ion.facet.Facets;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.SimpleCatalog;
import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class IonInfrastructureFuzzer {

    @FuzzTest(maxDuration = "15m")
    public void myFuzzTest(FuzzedDataProvider data) {
        // Strategy 1: Custom Catalog and SystemBuilder configuration
        SimpleCatalog catalog = new SimpleCatalog();
        
        // Populate catalog with some random shared symtabs
        int symtabs = data.consumeInt(0, 5);
        for (int i = 0; i < symtabs; i++) {
            String name = data.consumeString(10);
            int version = data.consumeInt(1, 10);
            try {
                // We need a real shared symtab, but creating one usually requires a system or loader
                // For now, let's just use the catalog logic with the system we build
            } catch (Exception e) {}
        }

        IonSystemBuilder builder = IonSystemBuilder.standard()
                .withCatalog(catalog)
                .withStreamCopyOptimized(data.consumeBoolean());
        
        IonSystem ionSys = builder.build();
        byte[] input = data.consumeBytes(data.consumeInt(0, 2000));
        if (input.length == 0) return;

        try {
            // Strategy 2: Test SeekableReader and Spans via Facets
            try (IonReader reader = ionSys.newReader(input)) {
                SeekableReader seekable = Facets.asFacet(SeekableReader.class, reader);
                if (seekable != null) {
                    while (reader.next() != null) {
                        try {
                            Span span = Facets.asFacet(Span.class, reader);
                            if (span != null && data.consumeBoolean()) {
                                seekable.hoist(span);
                            }
                        } catch (Exception e) {}
                    }
                }
            }
        } catch (Exception e) {}

        try {
            // Strategy 3: Compare Lite vs Standard System for same input
            IonSystem liteSys = IonSystemBuilder.standard().build(); // Lite is default in standard builder
            
            IonDatagram dg1 = ionSys.getLoader().load(input);
            IonDatagram dg2 = liteSys.getLoader().load(input);
            
            // Exercise equivalence and hashcode across systems
            dg1.equals(dg2);
            dg1.hashCode();
        } catch (Exception e) {}

        try {
            // Strategy 4: IonWriter with different configurations
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (IonWriter writer = ionSys.newBinaryWriter(baos)) {
                // Perform some writes that might trigger experimental optimizations
                IonReader reader = ionSys.newReader(input);
                writer.writeValues(reader);
            }
        } catch (Exception e) {}
    }
}
