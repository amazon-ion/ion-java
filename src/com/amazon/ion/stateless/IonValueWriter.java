package com.amazon.ion.stateless;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public abstract class IonValueWriter<T> {


    public void writeValue(int[] annotations, T value, ByteArrayOutputStream out) throws IOException {
        int vlength = value==null?0:getLength(value);
        if (annotations != null && annotations.length>0)
            StatelessWriter.writeAnnotations(annotations, getTypeDescriptorAndLengthOverhead(vlength)+vlength, out);

        if (value == null)
            writeNull(out);
        else
            writeValue(value, vlength, out);
    }

    void writeNull(ByteArrayOutputStream out) {
        int hn = getTid(null);
        int ln = IonConstants.lnIsNullAtom;
        StatelessWriter.writeTypeDescriptor(hn,ln,out);
    }

    void writeValue(T value, int vlength, ByteArrayOutputStream out) throws IOException {
        int hn = getTid(value);
        int ln = computeLowNibble(value, vlength);
        StatelessWriter.writeTypeDescriptor(hn, ln,out);

        if (vlength > 0) {
            switch (ln) {
            case IonConstants.lnIsNullAtom:
            case IonConstants.lnNumericZero:
                break;  // we don't need to do anything here
            case IonConstants.lnIsVarLen:
                StatelessWriter.writeVarUInt7Value(vlength, true, out);
            default:
                doWriteNakedValue(value, out, vlength);
                break;
            }
        }
    }

    abstract void doWriteNakedValue(T value, ByteArrayOutputStream out, int vlength) throws IOException;

    abstract int getLength(T value);

    int getTypeDescriptorAndLengthOverhead(int length) {
        return IonConstants.BB_TOKEN_LEN + IonBinary.lenLenFieldWithOptionalNibble(length);
    }

    abstract int getTid(T value);

    int computeLowNibble(T value, int valueLength) {
        int ln;
        ln = valueLength;
        if (ln > IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
        }
        return ln;
    }

}
