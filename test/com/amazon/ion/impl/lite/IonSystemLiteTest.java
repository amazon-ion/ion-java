package com.amazon.ion.impl.lite;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

class IonSystemLiteTest {
    // Confirms that the standard IonSystem can materialize Ion data that is deeply nested without
    // exhausting the stack.
    @Test
    void materializeDeeplyNested() {
        final int levelsOfNesting = 5_000_000;
        final StringBuilder ionText = new StringBuilder(levelsOfNesting * 2);
        for (int m = 0; m < levelsOfNesting; m++) {
            ionText.append('[');
        }
        for (int m = 0; m < levelsOfNesting; m++) {
            ionText.append(']');
        }
        IonSystem ionSystem = IonSystemBuilder.standard().build();
        Iterator<IonValue> iterator = ionSystem.iterate(ionText.toString());
        try {
            iterator.next();
        } catch (Exception e) {
            fail("Encountered an exception: " + e);
        }
    }
}