package software.amazon.ion.impl.lite;

import software.amazon.ion.IonSequence;

public class IonDatagramLiteTest extends BaseIonSequenceLiteTestCase {
    @Override
    protected IonSequence newEmptySequence() {
        return SYSTEM.newDatagram();
    }
}
