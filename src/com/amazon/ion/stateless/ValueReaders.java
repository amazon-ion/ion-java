package com.amazon.ion.stateless;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Date;

import com.amazon.ion.IonException;

public class ValueReaders {

    static final class symbolReader extends IonValueReader<Integer> {

        @Override
        Integer readValue(int tid, int ln, ByteBuffer in) throws IOException {
            if (tid != IonConstants.tidSymbol) {
                throw new IonException("invalid type desc encountered for value");
            }
            int length = ln;
            if (ln == IonConstants.lnIsVarLen)
                length = StatelessReader.readVarUInt7IntValue(in);

            return StatelessReader.readVarUInt8IntValue(in, length);
        }

    }

    public static final class nullReader extends IonValueReader<Object> {
        @Override
        Object readValue(int tid, int ln, ByteBuffer in) throws IOException {
            throw new IonException("Null that isn't null.");
        }
    }

    public static final class booleanReader extends IonValueReader<Boolean> {

        @Override
        Boolean readValue(int tid, int ln, ByteBuffer in) throws IOException {
            if (ln == 0)
                return false;
            if (ln == 1)
                return true;
            throw new IonException("Boolean that is not true or false.");
        }

    }

    public static final class byteReader extends IonValueReader<Byte> {
        @Override
        Byte readValue(int tid, int ln, ByteBuffer in) throws IOException {
            int length = ln;
            if (ln == IonConstants.lnIsVarLen)
                length = StatelessReader.readVarUInt7IntValue(in);

            long result = StatelessReader.readVarUInt8LongValue(in, length);
            if (tid == IonConstants.tidNegInt) {
                result = -result;
            }
            return (byte) result;
        }
    }

    public static final class shortReader extends IonValueReader<Short> {

        @Override
        Short readValue(int tid, int ln, ByteBuffer in) throws IOException {
            int length = ln;
            if (ln == IonConstants.lnIsVarLen)
                length = StatelessReader.readVarUInt7IntValue(in);

            long result = StatelessReader.readVarUInt8LongValue(in, length);
            if (tid == IonConstants.tidNegInt) {
                result = -result;
            }
            return (short) result;
        }

    }

    public static final class intReader extends IonValueReader<Integer> {

        @Override
        Integer readValue(int tid, int ln, ByteBuffer in) throws IOException {
            int length = ln;
            if (ln == IonConstants.lnIsVarLen)
                length = StatelessReader.readVarUInt7IntValue(in);

            long result = StatelessReader.readVarUInt8LongValue(in, length);
            if (tid == IonConstants.tidNegInt) {
                result = -result;
            }
            return (int) result;
        }

    }

    public static final class longReader extends IonValueReader<Long> {

        @Override
        Long readValue(int tid, int ln, ByteBuffer in) throws IOException {
            int length = ln;
            if (ln == IonConstants.lnIsVarLen)
                length = StatelessReader.readVarUInt7IntValue(in);

            long result = StatelessReader.readVarUInt8LongValue(in, length);
            if (tid == IonConstants.tidNegInt) {
                result = -result;
            }
            return result;
        }

    }

    public static final class floatReader extends IonValueReader<Float> {

        @Override
        Float readValue(int tid, int ln, ByteBuffer in) throws IOException {
            if (tid != IonConstants.tidFloat) {
                throw new IonException("invalid type desc encountered for float");
            }
            if (ln == IonConstants.lnNumericZero)
                return 0f;
            int length = ln;
            if (ln == IonConstants.lnIsVarLen)
                length = StatelessReader.readVarUInt7IntValue(in);

            return (float) StatelessReader.readFloatValue(in, length);
        }
    }

    public static final class doubleReader extends IonValueReader<Double> {

        @Override
        Double readValue(int tid, int ln, ByteBuffer in) throws IOException {
            if (tid != IonConstants.tidFloat) {
                throw new IonException("invalid type desc encountered for float");
            }
            if (ln == IonConstants.lnNumericZero)
                return 0d;
            int length = ln;
            if (ln == IonConstants.lnIsVarLen)
                length = StatelessReader.readVarUInt7IntValue(in);

            return StatelessReader.readFloatValue(in, length);
        }

    }

    public static final class stringReader extends IonValueReader<String> {
        static final Charset _encoding = Charset.forName("UTF-8");
        static final CharsetDecoder _decoder = _encoding.newDecoder();

        @Override
        String readValue(int tid, int ln, ByteBuffer in) throws IOException {
            if (tid != IonConstants.tidString) {
                throw new IonException("invalid type desc encountered for value");
            }
            int length = ln;
            if (ln == IonConstants.lnIsVarLen)
                length = StatelessReader.readVarUInt7IntValue(in);
            int limit = in.limit();
            in.limit(in.position()+length);
            CharBuffer result = _decoder.decode(in);
            in.limit(limit);
            return result.toString();
           // return StatelessReader.readString(in, length);
        }
    }

    public static final class dateReader extends IonValueReader<Date> {

        @Override
        Date readValue(int tid, int ln, ByteBuffer in) throws IOException {
            if (tid != IonConstants.tidTimestamp) {
                throw new IonException("invalid type desc encountered for value");
            }
            int length = ln;
            if (ln == 0)
                return new Date(0L);

            if (ln == IonConstants.lnIsVarLen)
                length = StatelessReader.readVarUInt7IntValue(in);
            return StatelessReader.readTimestampValue(in,length).d;
        }

    }

    static final class clobReader extends IonValueReader<byte[]> {

        @Override
        byte[] readValue(int tid, int ln, ByteBuffer in) throws IOException {
            if (tid != IonConstants.tidClob && tid != IonConstants.tidBlob) {
                throw new IonException("invalid type desc encountered for value");
            }
            int length = ln;
            if (ln == IonConstants.lnIsVarLen)
                length = StatelessReader.readVarUInt7IntValue(in);
            byte[] result = new byte[length];
            in.get(result);
            return result;
        }

    }

    public static final class blobReader extends IonValueReader<byte[]> {

        @Override
        byte[] readValue(int tid, int ln, ByteBuffer in) throws IOException {
            if (tid != IonConstants.tidClob && tid != IonConstants.tidBlob) {
                throw new IonException("invalid type desc encountered for value");
            }
            int length = ln;
            if (ln == IonConstants.lnIsVarLen)
                length = StatelessReader.readVarUInt7IntValue(in);
            byte[] result = new byte[length];
            in.get(result);
            return result;
        }
    }
}
