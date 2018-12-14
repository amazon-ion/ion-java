package software.amazon.ion.impl.lite;

import software.amazon.ion.IonSequence;

public class IonListLiteTest extends BaseIonSequenceLiteTestCase {
    @Override
    protected IonSequence newEmptySequence() {
        return SYSTEM.newEmptyList();
    }
}
