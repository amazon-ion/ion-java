package com.amazon.ion.stateless;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ContainerWriters {

    public static final class structWriter {
        public void writeKey(int symbolID, ByteArrayOutputStream out) throws IOException {
            StatelessWriter.writeVarUInt7Value(symbolID, true, out);
        }

        public void writeStructStart(int[] annotations, StatelessSymbolTable localSymbols, int length,
                ByteArrayOutputStream out) throws IOException {
            if (localSymbols != null)
                throw new UnsupportedOperationException("Don't yet have support for symbol tables");
            if (annotations != null && annotations.length > 0)
                StatelessWriter.writeAnnotations(annotations, getTypeDescriptorAndLengthOverhead(length)
                        + length, out);
            int hn = IonConstants.tidStruct;
            StatelessWriter.writeCommonHeader(hn, length, out);
        }

        public void writeStructEnd(ByteArrayOutputStream out) {
            // Do nothing in the binary format.
        }
    }

    public static final class listWriter {
        public void writeListStart(int[] annotations, int length, ByteArrayOutputStream out) throws IOException {
            if (annotations != null && annotations.length > 0)
                StatelessWriter.writeAnnotations(annotations, getTypeDescriptorAndLengthOverhead(length)
                        + length, out);
            int hn = IonConstants.tidList;
            StatelessWriter.writeCommonHeader(hn, length, out);
        }

        public void writeListEnd(ByteArrayOutputStream out) {
            // Do nothing in the binary format.
        }
    }

    static private int getTypeDescriptorAndLengthOverhead(int length) {
        return IonConstants.BB_TOKEN_LEN + IonBinary.lenLenFieldWithOptionalNibble(length);
    }

}
