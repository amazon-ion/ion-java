package com.amazon.ion.stateless;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.amazon.ion.IonException;
import com.amazon.ion.IonType;

public final class ContainerReaders {

    public static final class structReader {
        /**
         * Reads the beginning of a nested struct.
         * @param in The buffer to be read from.
         * The buffer position will be advanced to point to the first element of the nested structure.
         * The limit of the buffer will be set to the end of the nested struct.
         * @return If this struct has an attached symbol table, it is read and returned.
         * @throws IOException
         */
        public StatelessSymbolTable readStructStart(ByteBuffer in) throws IOException {
            int length = StatelessReader.readLength(in,IonConstants.tidStruct);
            if (in.position()+length > in.limit())
                throw new IonException("Struct goes off end of buffer.");
            in.limit(in.position()+length);
            return null;
        }

        public int readKey(ByteBuffer in) throws IOException {
            return StatelessReader.readVarUInt7IntValue(in);
        }

        public int[] readAnnotations(ByteBuffer buff) throws IOException {
            return ContainerReaders.readAnnotations(buff);
        }

        public IonType readType(ByteBuffer in) throws IOException {
            return StatelessReader.readActualTypeDesc(in);
        }

        public void readStructEnd(int oldLimit, ByteBuffer buff) {
            buff.limit(oldLimit);
        }
    }

    public static final class listReader {
        /**
         * Reads the beginning of a nested list.
         * @param buff The buffer to be read from.
         * The buffer position will be advanced to point to the first element of the nested list.
         * The limit of the buffer will be set to the end of the nested list.
         * @return The number of elements in this list.
         * @throws IOException
         */
        public int readListStart(ByteBuffer in) throws IOException {
            int length = StatelessReader.readLength(in,IonConstants.tidList);
            if (in.position()+length > in.limit())
                throw new IonException("List goes off end of buffer.");
            in.limit(in.position()+length);
            return StatelessReader.getNumElementsInList(length, in);
        }

        public int[] readAnnotations(ByteBuffer buff) throws IOException {
            return ContainerReaders.readAnnotations(buff);
        }

        public IonType readType(ByteBuffer in) throws IOException {
            return StatelessReader.readActualTypeDesc(in);
        }

        public void readListEnd(int oldLimit, ByteBuffer buff) {
            buff.limit(oldLimit);
        }
    }

    static final int[] readAnnotations(ByteBuffer in) throws IOException {
        return StatelessReader.readAnnotations(in);
    }
}
