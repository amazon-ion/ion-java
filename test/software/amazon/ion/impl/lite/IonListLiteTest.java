package software.amazon.ion.impl.lite;

import software.amazon.ion.IonSequence;

public class IonListLiteTest extends BaseIonSequenceLiteTest {
    @Override
    protected IonSequence newEmptySequence() {
        return SYSTEM.newDatagram();
    }
}
