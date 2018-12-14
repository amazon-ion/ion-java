package software.amazon.ion.impl.lite;

import software.amazon.ion.IonSequence;

public class IonSexpLiteTest extends BaseIonSequenceLiteTestCase {
    @Override
    protected IonSequence newEmptySequence() {
        return SYSTEM.newEmptySexp();
    }
}
