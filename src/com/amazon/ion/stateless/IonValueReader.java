package com.amazon.ion.stateless;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class IonValueReader <T> {

    public T readValue(ByteBuffer in) throws IOException {
        int c = StatelessReader.readToken(in);
        int tid = IonConstants.getTypeCode(c);
        int ln = IonConstants.getLowNibble(c);
        if (ln == IonConstants.lnIsNullAtom)
            return null;
        return readValue(tid, ln,in);
    }

    abstract T readValue(int tid, int ln, ByteBuffer in) throws IOException;

}
