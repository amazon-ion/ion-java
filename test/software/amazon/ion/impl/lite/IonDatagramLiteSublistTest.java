package software.amazon.ion.impl.lite;

import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonSequence;

public class IonDatagramLiteSublistTest extends BaseIonSequenceLiteSublistTestCase {

    protected IonSequence newSequence() {
        final IonDatagram datagram = SYSTEM.newDatagram();
        for(int i : INTS) {
            datagram.add(SYSTEM.newInt(INTS[i]));
        }

        return datagram;
    }
}
