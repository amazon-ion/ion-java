package com.amazon.ion.stateless;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Date;

import com.amazon.ion.impl.IonTokenReader.Type.timeinfo;

public class ValueWriters {

    public static final class symbolWriter extends IonValueWriter<Integer> {

        @Override
        void doWriteNakedValue(Integer value, ByteArrayOutputStream out, int vlength) throws IOException {
            int wlen = StatelessWriter.writeVarUInt8Value(value, vlength,out);
            assert wlen == vlength;
        }

        @Override
        int getLength(Integer value) {
            return IonBinary.lenVarUInt8(value);
        }

        @Override
        int getTid(Integer value) {
            return IonConstants.tidSymbol;
        }

    }

    public static final class nullWriter extends IonValueWriter<Object> {

        @Override
        void doWriteNakedValue(Object value, ByteArrayOutputStream out, int vlength) throws IOException {
            throw new IllegalStateException("Method should never be called.");
        }

        @Override
        int getLength(Object value) {
            throw new IllegalStateException("Method should never be called.");
        }

        @Override
        int getTid(Object value) {
            return IonConstants.tidNull;
        }

    }

    public static final class booleanWriter extends IonValueWriter<Boolean> {

        @Override
        void doWriteNakedValue(Boolean value, ByteArrayOutputStream out, int vlength) throws IOException {
            //Do nothing.
        }

        @Override
        int getLength(Boolean value) {
            return 0;
        }

        @Override
        int computeLowNibble (Boolean value,int length) {
            if (Boolean.FALSE.equals(value)) {
                return 0;
            } else if (Boolean.TRUE.equals(value)) {
                return 1;
            } else {
                throw new IllegalStateException("Boolean that is not true or false.");
            }
        }

        @Override
        int getTid(Boolean value) {
            return IonConstants.tidBoolean;
        }

    }

    public static final class byteWriter extends IonValueWriter<Byte> {

        @Override
        void doWriteNakedValue(Byte value, ByteArrayOutputStream out, int vlength) throws IOException {
            long l = (value < 0) ? -value : value;
            int wlen = StatelessWriter.writeVarUInt8Value(l, vlength, out);
            assert wlen == vlength;
        }

        @Override
        int getLength(Byte value) {
            return IonBinary.lenIonInt(value);
        }

        @Override
        int getTid(Byte value) {
            if (value < 0 ) {
                return IonConstants.tidNegInt;
            }
            return IonConstants.tidPosInt;
        }

    }

    public static final class shortWriter extends IonValueWriter<Short> {

        @Override
        void doWriteNakedValue(Short value, ByteArrayOutputStream out, int vlength) throws IOException {
            long l = (value < 0) ? -value : value;
            int wlen = StatelessWriter.writeVarUInt8Value(l, vlength, out);
            assert wlen == vlength;
        }

        @Override
        int getLength(Short value) {
            return IonBinary.lenIonInt(value);
        }

        @Override
        int getTid(Short value) {
            if (value < 0 ) {
                return IonConstants.tidNegInt;
            }
            return IonConstants.tidPosInt;
        }

    }

    public static final class intWriter extends IonValueWriter<Integer> {

        @Override
        void doWriteNakedValue(Integer value, ByteArrayOutputStream out, int vlength) throws IOException {
            long l = (value < 0) ? -value : value;
            int wlen = StatelessWriter.writeVarUInt8Value(l, vlength, out);
            assert wlen == vlength;
        }

        @Override
        int getLength(Integer value) {
            return IonBinary.lenIonInt(value);
        }

        @Override
        int getTid(Integer value) {
            if (value < 0 ) {
                return IonConstants.tidNegInt;
            }
            return IonConstants.tidPosInt;
        }

    }

    public static final class longWriter extends IonValueWriter<Long> {

        @Override
        void doWriteNakedValue(Long value, ByteArrayOutputStream out, int vlength) throws IOException {
            long l = (value < 0) ? -value : value;
            int wlen = StatelessWriter.writeVarUInt8Value(l, vlength, out);
            assert wlen == vlength;
        }

        @Override
        int getLength(Long value) {
            return IonBinary.lenIonInt(value);
        }

        @Override
        int getTid(Long value) {
            if (value < 0 ) {
                return IonConstants.tidNegInt;
            }
            return IonConstants.tidPosInt;
        }

    }

    public static final class floatWriter extends IonValueWriter<Float> {

        @Override
        void doWriteNakedValue(Float value, ByteArrayOutputStream out, int vlength) throws IOException {
            int wlen = StatelessWriter.writeFloatValue(value,out);
            assert wlen == vlength;
        }

        @Override
        int getLength(Float value) {
            return IonBinary.lenIonFloat(value);
        }

        @Override
        int getTid(Float value) {
            return IonConstants.tidFloat;
        }

    }

    public static final class doubleWriter extends IonValueWriter<Double> {

        @Override
        void doWriteNakedValue(Double value, ByteArrayOutputStream out, int vlength) throws IOException {
            int wlen = StatelessWriter.writeFloatValue(value,out);
            assert wlen == vlength;
        }

        @Override
        int getLength(Double value) {
            return IonBinary.lenIonFloat(value);
        }

        @Override
        int getTid(Double value) {
            return IonConstants.tidFloat;
        }

    }
    public static final class stringWriter extends IonValueWriter<String> {
        static final Charset _encoding = Charset.forName("UTF-8");
        static final CharsetEncoder _encoder = _encoding.newEncoder();

        @Override
        public void writeValue(int[] annotations, String value, ByteArrayOutputStream out) throws IOException {
            if (value == null) {
                if (annotations != null && annotations.length>0)
                    StatelessWriter.writeAnnotations(annotations, getTypeDescriptorAndLengthOverhead(0), out);
                writeNull(out);
            } else {
                ByteBuffer encoded = StatelessWriter.writeStringData(value);
                int vlength = encoded.remaining();

                if (annotations != null && annotations.length>0)
                    StatelessWriter.writeAnnotations(annotations, getTypeDescriptorAndLengthOverhead(vlength)+vlength, out);

                int hn = getTid(value);
                int ln = computeLowNibble(value, vlength);
                StatelessWriter.writeTypeDescriptor(hn, ln,out);
                if (vlength > 0) {
                    if (ln == IonConstants.lnIsVarLen)
                        StatelessWriter.writeVarUInt7Value(vlength, true, out);
                    out.write(encoded.array(), encoded.arrayOffset(), encoded.remaining());
                }
            }
        }

        @Override
        @Deprecated
        void doWriteNakedValue(String value, ByteArrayOutputStream out, int vlength) throws IOException {
            throw new IllegalStateException("Method should never be called");
        }

        @Override
        @Deprecated
        int getLength(String value) {
            throw new IllegalStateException("Method should never be called");
        }

        @Override
        int getTid(String value) {
            return IonConstants.tidString;
        }

    }
    public static final class dateWriter extends IonValueWriter<Date> {

        @Override
        void doWriteNakedValue(Date value, ByteArrayOutputStream out, int vlength) throws IOException {
            int wlen = StatelessWriter.writeTimestamp(new timeinfo(value, null),out);
            assert wlen == vlength;
        }

        @Override
        int getLength(Date value) {
            return IonBinary.lenIonTimestamp(new timeinfo(value, null));
        }

        @Override
        int getTid(Date value) {
            return IonConstants.tidTimestamp;
        }

    }
    public static final class clobWriter extends IonValueWriter<byte[]> {

        @Override
        void doWriteNakedValue(byte[] value, ByteArrayOutputStream out, int vlength) throws IOException {
            out.write(value, 0, vlength);
        }

        @Override
        int getLength(byte[] value) {
            return value.length;
        }

        @Override
        int getTid(byte[] value) {
            return IonConstants.tidClob;
        }

    }
    public static final class blobWriter extends IonValueWriter<byte[]> {

        @Override
        void doWriteNakedValue(byte[] value, ByteArrayOutputStream out, int vlength) throws IOException {
            out.write(value,0,vlength);
        }

        @Override
        int getLength(byte[] value) {
            return value.length;
        }

        @Override
        int getTid(byte[] value) {
            return IonConstants.tidBlob;
        }

    }
}
