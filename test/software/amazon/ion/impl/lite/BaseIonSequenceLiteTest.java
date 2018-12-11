package software.amazon.ion.impl.lite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import org.junit.Test;
import software.amazon.ion.IonSequence;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonValue;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.system.IonSystemBuilder;

public abstract class BaseIonSequenceLiteTest {

    protected static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    protected abstract IonSequence newEmptySequence();

    @Test
    public void retainAll() {
        final IonSequence sequence = newEmptySequence();
        final ArrayList<IonValue> toRetain = new ArrayList<IonValue>();

        final IonValue retainedValue = SYSTEM.newInt(1);
        sequence.add(retainedValue);
        toRetain.add(retainedValue);

        final IonValue toRemoveValue = SYSTEM.newInt(2);
        sequence.add(toRemoveValue);

        assertTrue(sequence.retainAll(toRetain));

        assertEquals(1, sequence.size());
        assertTrue(sequence.contains(retainedValue));
        assertFalse(sequence.contains(toRemoveValue));
    }

    @Test
    public void retainAllUsesReferenceEquality() {
        final IonSequence sequence = newEmptySequence();
        final ArrayList<IonValue> toRetain = new ArrayList<IonValue>();

        final IonValue value = SYSTEM.newInt(1);
        sequence.add(value);

        final IonValue equalValue = SYSTEM.newInt(1);
        toRetain.add(equalValue);

        assertEquals(equalValue, value);
        assertNotSame(equalValue, value);

        assertTrue(sequence.retainAll(toRetain));
        assertEquals(0, sequence.size());
    }

    @Test(expected = ReadOnlyValueException.class)
    public void retainAllReadOnly() {
        final IonSequence sequence = newEmptySequence();
        sequence.makeReadOnly();

        sequence.retainAll(Collections.emptyList());
    }
}