/*
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl.lite;

import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static software.amazon.ion.SystemSymbols.IMPORTS_SID;
import static software.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static software.amazon.ion.SystemSymbols.MAX_ID_SID;
import static software.amazon.ion.SystemSymbols.NAME_SID;
import static software.amazon.ion.SystemSymbols.SYMBOLS_SID;
import static software.amazon.ion.SystemSymbols.VERSION_SID;
import static software.amazon.ion.impl.PrivateIonConstants.BINARY_VERSION_MARKER_1_0;
import static software.amazon.ion.impl.PrivateIonConstants.lnBooleanFalse;
import static software.amazon.ion.impl.PrivateIonConstants.lnBooleanTrue;
import static software.amazon.ion.impl.PrivateIonConstants.lnIsNull;
import static software.amazon.ion.impl.PrivateIonConstants.lnIsVarLen;
import static software.amazon.ion.impl.PrivateIonConstants.tidBlob;
import static software.amazon.ion.impl.PrivateIonConstants.tidBoolean;
import static software.amazon.ion.impl.PrivateIonConstants.tidClob;
import static software.amazon.ion.impl.PrivateIonConstants.tidDecimal;
import static software.amazon.ion.impl.PrivateIonConstants.tidFloat;
import static software.amazon.ion.impl.PrivateIonConstants.tidList;
import static software.amazon.ion.impl.PrivateIonConstants.tidNegInt;
import static software.amazon.ion.impl.PrivateIonConstants.tidNull;
import static software.amazon.ion.impl.PrivateIonConstants.tidPosInt;
import static software.amazon.ion.impl.PrivateIonConstants.tidSexp;
import static software.amazon.ion.impl.PrivateIonConstants.tidString;
import static software.amazon.ion.impl.PrivateIonConstants.tidStruct;
import static software.amazon.ion.impl.PrivateIonConstants.tidSymbol;
import static software.amazon.ion.impl.PrivateIonConstants.tidTimestamp;
import static software.amazon.ion.impl.PrivateIonConstants.tidTypedecl;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.ListIterator;
import software.amazon.ion.Decimal;
import software.amazon.ion.IonBlob;
import software.amazon.ion.IonBool;
import software.amazon.ion.IonClob;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonDecimal;
import software.amazon.ion.IonException;
import software.amazon.ion.IonFloat;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonList;
import software.amazon.ion.IonSequence;
import software.amazon.ion.IonSexp;
import software.amazon.ion.IonString;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonTimestamp;
import software.amazon.ion.IonValue;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Timestamp;


/**
 * Encoder implementation that encodes a IonDatagram into binary format using a
 * reverse encoding algorithm instead of the default pre-order (left-to-right)
 * two-pass algorithm.
 * <p>
 * This reverse encoding algorithm requires a fully materialized IonDatagram
 * DOM to qualify for use. It uses a single buffer, {@link #myBuffer}, to hold
 * the entire binary-encoded data, with an integer, {@link #myOffset}, to index
 * the current position to write the bytes.
 * <p>
 * The algorithm begins by traversing from the last top-level value to the
 * first top-level value. During this traversal, it recursively goes into the
 * nested values of the top-level value being traversed in a similar
 * last-to-first (right-to-left) order.
 */
class ReverseBinaryEncoder
{
    private static final BigInteger MAX_LONG_VALUE =
        BigInteger.valueOf(Long.MAX_VALUE);

    private static final int NULL_LENGTH_MASK   = lnIsNull;

    private static final int TYPE_NULL          = tidNull       << 4;
    private static final int TYPE_BOOL          = tidBoolean    << 4;
    private static final int TYPE_POS_INT       = tidPosInt     << 4;
    private static final int TYPE_NEG_INT       = tidNegInt     << 4;
    private static final int TYPE_FLOAT         = tidFloat      << 4;
    private static final int TYPE_DECIMAL       = tidDecimal    << 4;
    private static final int TYPE_TIMESTAMP     = tidTimestamp  << 4;
    private static final int TYPE_SYMBOL        = tidSymbol     << 4;
    private static final int TYPE_STRING        = tidString     << 4;
    private static final int TYPE_CLOB          = tidClob       << 4;
    private static final int TYPE_BLOB          = tidBlob       << 4;
    private static final int TYPE_LIST          = tidList       << 4;
    private static final int TYPE_SEXP          = tidSexp       << 4;
    private static final int TYPE_STRUCT        = tidStruct     << 4;
    private static final int TYPE_ANNOTATIONS   = tidTypedecl   << 4;

    /**
     * Holds the entire binary encoded data. When IonDatagram is fully encoded
     * into binary data, this byte array will hold that data.
     */
    private byte[] myBuffer;

    /**
     * Index onto the position where the bytes are last written to the buffer.
     * That means that if you want to write 1 more byte to the buffer, you have
     * to decrease the index by 1 (myOffset - 1).
     */
    private int myOffset;

    /**
     * The symbol table attached to the IonValue (and its nested values)
     * that the encoder is currently traversing on.
     */
    private SymbolTable mySymbolTable;

    private IonSystem myIonSystem;

    ReverseBinaryEncoder(int initialSize)
    {
        myBuffer = new byte[initialSize];
        myOffset = initialSize;
    }

    /**
     * Returns the size of the Ion binary-encoded byte array.
     * <p>
     * This makes an unchecked assumption that {{@link #serialize(IonDatagram)}
     * is already called.
     *
     * @return the number of bytes of the byte array
     */
    int byteSize()
    {
        return myBuffer.length - myOffset;
    }

    /**
     * Copies the current contents of the Ion binary-encoded byte array into a
     * new byte array. The allocates an array of the size needed to exactly hold
     * the output and copies the entire byte array to it.
     * <p>
     * This makes an unchecked assumption that {{@link #serialize(IonDatagram)}
     * is already called.
     *
     * @return the newly allocated byte array
     */
    byte[] toNewByteArray()
    {
        int length = myBuffer.length - myOffset;
        byte[] bytes = new byte[length];
        System.arraycopy(myBuffer, myOffset, bytes, 0, length);
        return bytes;
    }

    /**
     * Copies the current contents of the Ion binary-encoded byte array to a
     * specified stream.
     * <p>
     * This makes an unchecked assumption that {{@link #serialize(IonDatagram)}
     * is already called.
     *
     * @return the number of bytes written into {@code out}
     *
     * @throws IOException
     */
    int writeBytes(OutputStream out)
        throws IOException
    {
        int length = myBuffer.length - myOffset;
        byte[] bytes = new byte[length];
        System.arraycopy(myBuffer, myOffset, bytes, 0, length);
        out.write(bytes);
        return length;
    }

    /**
     * Serialize the IonDatagram into Ion binary-encoding, to the internal
     * byte array buffer of the encoder.
     * <p>
     * If the IonDatagram has been modified after this method call, you
     * <em>must</em> call this method again to correctly reflect the
     * modifications.
     *
     * @throws IonException
     */
    void serialize(IonDatagram dg)
        throws IonException
    {
        myIonSystem = dg.getSystem();
        mySymbolTable = null;

        // Write all top-level values in reverse
        writeIonValue(dg);

        // After all top-level values are written, write the local symbol table
        // that is attached to the top-level value that has just been written,
        // if it exists.
        if (mySymbolTable != null && mySymbolTable.isLocalTable()) {
            writeLocalSymbolTable(mySymbolTable);
        }

        // Write IVM
        writeBytes(BINARY_VERSION_MARKER_1_0);
    }

    void serialize(SymbolTable symTab)
        throws IonException
    {
        writeLocalSymbolTable(symTab);
    }

    /**
     * Grows the current buffer and returns the updated offset.
     *
     * @param offset the original offset
     * @return the updated offset
     */
    private int growBuffer(int offset)
    {
        assert offset < 0;
        byte[] oldBuf = myBuffer;
        int oldLen = oldBuf.length;
        byte[] newBuf = new byte[(-offset + oldLen) << 1]; // Double the buffer
        int oldBegin = newBuf.length - oldLen;
        System.arraycopy(oldBuf, 0, newBuf, oldBegin, oldLen);
        myBuffer = newBuf;
        myOffset += oldBegin;
        return offset + oldBegin;
    }

    /**
     * Writes the IonValue and its nested values recursively, including
     * annotations.
     *
     * @param value
     * @throws IonException
     */
    private void writeIonValue(IonValue value)
        throws IonException
    {
        final int valueOffset = myBuffer.length - myOffset;

        switch (value.getType())
        {
            // scalars
            case BLOB:      writeIonBlobContent((IonBlob) value);            break;
            case BOOL:      writeIonBoolContent((IonBool) value);            break;
            case CLOB:      writeIonClobContent((IonClob) value);            break;
            case DECIMAL:   writeIonDecimalContent((IonDecimal) value);      break;
            case FLOAT:     writeIonFloatContent((IonFloat) value);          break;
            case INT:       writeIonIntContent((IonInt) value);              break;
            case NULL:      writeIonNullContent();                           break;
            case STRING:    writeIonStringContent((IonString) value);        break;
            case SYMBOL:    writeIonSymbolContent((IonSymbol) value);        break;
            case TIMESTAMP: writeIonTimestampContent((IonTimestamp) value);  break;
            // containers
            case LIST:      writeIonListContent((IonList) value);            break;
            case SEXP:      writeIonSexpContent((IonSexp) value);            break;
            case STRUCT:    writeIonStructContent((IonStruct) value);        break;
            // IonDatagram
            case DATAGRAM:  writeIonDatagramContent((IonDatagram) value);    break;
            default:
                throw new IonException("IonType is unknown: " + value.getType());
        }

        writeAnnotations(value, valueOffset);
    }

    // =========================================================================
    // Basic Field Formats (Primitive Fields)
    // =========================================================================

    private void writeByte(int b)
    {
        int offset = myOffset;
        if (--offset < 0) {
            offset = growBuffer(offset);
        }
        // Using narrowing primitive conversion from int to byte
        myBuffer[offset] = (byte) b;
        myOffset = offset;
    }

    private void writeBytes(byte[] bytes)
    {
        int length = bytes.length;
        int offset = myOffset;
        if ((offset -= length) < 0) {
            offset = growBuffer(offset);
        }
        System.arraycopy(bytes, 0, myBuffer, offset, length);
        myOffset = offset;
    }

    private void writeUInt(long v)
    {
        int offset = myOffset;

        if (v < (1L << (8 * 1)))
        {
            if (--offset < 0) {
                offset = growBuffer(offset);
            }
            myBuffer[offset] = (byte) v;
        }
        else if (v < (1L << (8 * 2)))
        {
            offset -= 2;
            if (offset < 0) {
                offset = growBuffer(offset);
            }
            myBuffer[offset]   = (byte) (v >>> (8 * 1));
            myBuffer[offset+1] = (byte)  v;
        }
        else if (v < (1L << (8 * 3)))
        {
            offset -= 3;
            if (offset < 0) {
                offset = growBuffer(offset);
            }
            myBuffer[offset]   = (byte) (v >>> (8 * 2));
            myBuffer[offset+1] = (byte) (v >>> (8 * 1));
            myBuffer[offset+2] = (byte)  v;
        }
        else if (v < (1L << (8 * 4)))
        {
            offset -= 4;
            if (offset < 0) {
                offset = growBuffer(offset);
            }
            myBuffer[offset]   = (byte) (v >>> (8 * 3));
            myBuffer[offset+1] = (byte) (v >>> (8 * 2));
            myBuffer[offset+2] = (byte) (v >>> (8 * 1));
            myBuffer[offset+3] = (byte)  v;
        }
        else if (v < (1L << (8 * 5)))
        {
            offset -= 5;
            if (offset < 0) {
                offset = growBuffer(offset);
            }
            myBuffer[offset]   = (byte) (v >>> (8 * 4));
            myBuffer[offset+1] = (byte) (v >>> (8 * 3));
            myBuffer[offset+2] = (byte) (v >>> (8 * 2));
            myBuffer[offset+3] = (byte) (v >>> (8 * 1));
            myBuffer[offset+4] = (byte)  v;
        }
        else if (v < (1L << (8 * 6)))
        {
            offset -= 6;
            if (offset < 0) {
                offset = growBuffer(offset);
            }
            myBuffer[offset]   = (byte) (v >>> (8 * 5));
            myBuffer[offset+1] = (byte) (v >>> (8 * 4));
            myBuffer[offset+2] = (byte) (v >>> (8 * 3));
            myBuffer[offset+3] = (byte) (v >>> (8 * 2));
            myBuffer[offset+4] = (byte) (v >>> (8 * 1));
            myBuffer[offset+5] = (byte)  v;
        }
        else if (v < (1L << (8 * 7)))
        {
            offset -= 7;
            if (offset < 0) {
                offset = growBuffer(offset);
            }
            myBuffer[offset]   = (byte) (v >>> (8 * 6));
            myBuffer[offset+1] = (byte) (v >>> (8 * 5));
            myBuffer[offset+2] = (byte) (v >>> (8 * 4));
            myBuffer[offset+3] = (byte) (v >>> (8 * 3));
            myBuffer[offset+4] = (byte) (v >>> (8 * 2));
            myBuffer[offset+5] = (byte) (v >>> (8 * 1));
            myBuffer[offset+6] = (byte)  v;
        }
        else
        {
            offset -= 8;
            if (offset < 0) {
                offset = growBuffer(offset);
            }
            myBuffer[offset]   = (byte) (v >>> (8 * 7));
            myBuffer[offset+1] = (byte) (v >>> (8 * 6));
            myBuffer[offset+2] = (byte) (v >>> (8 * 5));
            myBuffer[offset+3] = (byte) (v >>> (8 * 4));
            myBuffer[offset+4] = (byte) (v >>> (8 * 3));
            myBuffer[offset+5] = (byte) (v >>> (8 * 2));
            myBuffer[offset+6] = (byte) (v >>> (8 * 1));
            myBuffer[offset+7] = (byte)  v;
        }

        myOffset = offset;
    }

    /**
     * Write a VarUInt field. VarUInts are sequence of bytes. The high-order
     * bit of the last octet is one, indicating the end of the sequence. All
     * other high-order bits must be zero.
     * <p>
     * Writes at least one byte, even for zero values. int parameter is enough
     * as the scalar and container writers do not have APIs that return long or
     * BigInteger representations.
     *
     * @param v
     */
    private void writeVarUInt(int v)
    {
        int offset = myOffset;

        if (v < (1 << (7 * 1)))               // 1 byte - 7 bits used - 0x7f max
        {
            if (--offset < 0) {
                offset = growBuffer(offset);
            }
            myBuffer[offset]     = (byte) (v | 0x80 );
        }
        else if (v < (1 << (7 * 2)))          // 2 bytes - 14 bits used - 0x3fff max
        {
            if ((offset -= 2) < 0) {
                offset = growBuffer(offset);
            }
            myBuffer[offset]     = (byte) (v >>> (7 * 1));
            myBuffer[offset + 1] = (byte) (v | 0x80);
        }
        else if (v < (1 << (7 * 3)))          // 3 bytes - 21 bits used - 0x1fffff max
        {
            if ((offset -= 3) < 0) {
                offset = growBuffer(offset);
            }
            myBuffer[offset]     = (byte) ( v >>> (7 * 2));
            myBuffer[offset + 1] = (byte) ((v >>> (7 * 1)) & 0x7f);
            myBuffer[offset + 2] = (byte) ( v | 0x80);
        }
        else if (v < (1 << (7 * 4)))          // 4 bytes - 28 bits used - 0xfffffff max
        {
            if ((offset -= 4) < 0) {
                offset = growBuffer(offset);
            }
            myBuffer[offset]     = (byte) ( v >>> (7 * 3));
            myBuffer[offset + 1] = (byte) ((v >>> (7 * 2)) & 0x7f);
            myBuffer[offset + 2] = (byte) ((v >>> (7 * 1)) & 0x7f);
            myBuffer[offset + 3] = (byte) ( v | 0x80);
        }
        else                                  // 5 bytes - 32 bits used - 0x7fffffff max (Integer.MAX_VALUE)
        {
            if ((offset -= 5) < 0) {
                offset = growBuffer(offset);
            }
            myBuffer[offset]     = (byte) ( v >>> (7 * 4));
            myBuffer[offset + 1] = (byte) ((v >>> (7 * 3)) & 0x7f);
            myBuffer[offset + 2] = (byte) ((v >>> (7 * 2)) & 0x7f);
            myBuffer[offset + 3] = (byte) ((v >>> (7 * 1)) & 0x7f);
            myBuffer[offset + 4] = (byte) ( v | 0x80);
        }

        myOffset = offset;
    }

    /**
     * Write a VarInt field. VarInts are sequence of bytes. The high-order bit
     * of the last octet is one, indicating the end of the sequence. All other
     * high-order bits must be zero. The second-highest order bit (0x40) is a
     * sign flag in the first octet of the representation, but part of the
     * extension bits for all other octets.
     * <p>
     * Writes at least one byte, even for zero values. int parameter is enough
     * as the scalar and container writers do not have APIs that return long or
     * BigInteger representations.
     *
     * @param v
     */
    private void writeVarInt(int v)
    {
        if (v == 0)
        {
            writeByte(0x80);
        }
        else
        {
            int offset = myOffset;

            boolean is_negative = (v < 0);
            if (is_negative)
            {
                // note that for Integer.MIN_VALUE (0x80000000) the negative
                // is the same, but that's also the bit pattern we need to
                // write out - so no worries
                v = -v;
            }

            if (v < (1 << (7 * 1 - 1)))           // 1 byte - 6 bits used - 0x3f max
            {
                if (--offset < 0) {
                    offset = growBuffer(offset);
                }
                if (is_negative)
                    v |= 0x40;
                myBuffer[offset]     = (byte) (v | 0x80);
            }
            else if (v < (1 << (7 * 2 - 1)))      // 2 bytes - 13 bits used - 0x1fff max
            {
                if ((offset -= 2) < 0) {
                    offset = growBuffer(offset);
                }
                if (is_negative)
                    v |= 0x2000;
                myBuffer[offset]     = (byte) (v >>> (7 * 1));
                myBuffer[offset + 1] = (byte) (v | 0x80);
            }
            else if (v < (1 << (7 * 3 - 1)))      // 3 bytes - 20 bits used - 0xfffff max
            {
                if ((offset -= 3) < 0) {
                    offset = growBuffer(offset);
                }
                if (is_negative)
                    v |= 0x100000;
                myBuffer[offset]     = (byte) ( v >>> (7 * 2));
                myBuffer[offset + 1] = (byte) ((v >>> (7 * 1)) & 0x7f);
                myBuffer[offset + 2] = (byte) ( v | 0x80);
            }
            else if (v < (1 << (7 * 4 - 1)))      // 4 bytes - 27 bits used - 0x7ffffff max
            {
                if ((offset -= 4) < 0) {
                    offset = growBuffer(offset);
                }
                if (is_negative)
                    v |= 0x8000000;
                myBuffer[offset]     = (byte) ( v >>> (7 * 3));
                myBuffer[offset + 1] = (byte) ((v >>> (7 * 2)) & 0x7f);
                myBuffer[offset + 2] = (byte) ((v >>> (7 * 1)) & 0x7f);
                myBuffer[offset + 3] = (byte) ( v | 0x80);
            }
            else                                  // 5 bytes - 31 bits used - 0x7fffffff max (Integer.MAX_VALUE)
            {
                if ((offset -= 5) < 0) {
                    offset = growBuffer(offset);
                }

                // This is different from the previous if-blocks because we
                // cannot represent a int with more than 32 bits to perform
                // the "OR-assignment" (|=).
                myBuffer[offset]     = (byte) ((v >>> (7 * 4)) & 0x7f);
                if (is_negative) {
                    myBuffer[offset] |= 0x40;
                }

                myBuffer[offset + 1] = (byte) ((v >>> (7 * 3)) & 0x7f);
                myBuffer[offset + 2] = (byte) ((v >>> (7 * 2)) & 0x7f);
                myBuffer[offset + 3] = (byte) ((v >>> (7 * 1)) & 0x7f);
                myBuffer[offset + 4] = (byte) ( v | 0x80);
            }

            myOffset = offset;
        }
    }

    // =========================================================================
    // Type Descriptors
    // =========================================================================

    /**
     * Writes the prefix (type and length) preceding the body of an encoded
     * value. This method is only called <em>after</em> a value's body is
     * written to the buffer.
     *
     * @param type
     *        the value's type, a four-bit high-nibble mask
     * @param length
     *        the number of bytes (octets) in the body, excluding the prefix
     *        itself
     */
    private void writePrefix(int type, int length)
    {
        if (length >= lnIsVarLen)
        {
            writeVarUInt(length);
            length = lnIsVarLen;
        }

        int offset = myOffset;
        if (--offset < 0) {
            offset = growBuffer(offset);
        }
        myBuffer[offset] = (byte) (type | length);
        myOffset = offset;
    }

    private void writeAnnotations(IonValue value, int endOfValueOffset)
    {
        SymbolToken[] annotationSymTokens = value.getTypeAnnotationSymbols();
        if (annotationSymTokens.length > 0)
        {
            final int annotatedValueOffset = myBuffer.length - myOffset;
            int sid;
            for (int i = annotationSymTokens.length; --i >= 0;)
            {
                sid = findSid(annotationSymTokens[i]);
                writeVarUInt(sid);
            }
            writeVarUInt(myBuffer.length - myOffset - annotatedValueOffset);
            writePrefix(TYPE_ANNOTATIONS,
                        myBuffer.length - myOffset - endOfValueOffset);
        }
    }

    // =========================================================================
    // Scalars
    // =========================================================================

    private void writeIonNullContent()
    {
        // null.null
        int encoded = TYPE_NULL | NULL_LENGTH_MASK;
        writeByte(encoded);
    }

    private void writeIonBoolContent(IonBool val)
    {
        int encoded;
        if (val.isNullValue())
        {
            encoded = TYPE_BOOL | NULL_LENGTH_MASK;
        }
        else
        {
            boolean b = val.booleanValue();
            encoded = b ? (TYPE_BOOL | lnBooleanTrue) :
                          (TYPE_BOOL | lnBooleanFalse);
        }
        writeByte(encoded);
    }

    private void writeIonIntContent(IonInt val)
    {
        if (val.isNullValue())
        {
            // NOTE: We are only writing the positive binary representation of
            // null value here.
            writeByte((byte) (TYPE_POS_INT | NULL_LENGTH_MASK));
        }
        else
        {
            BigInteger bigInt = val.bigIntegerValue();
            int signum = bigInt.signum();
            int type;
            final int originalOffset = myBuffer.length - myOffset;
            if (signum == 0)
            {
                // Zero has no bytes of data at all
                writeByte((byte) TYPE_POS_INT);
                return; // Finished writing IonInt as zero.
            }
            else if (signum < 0)
            {
                type = TYPE_NEG_INT;
                bigInt = bigInt.negate();
            }
            else
            {
                type = TYPE_POS_INT;
            }

            // Check the value if it's smaller than a long, if so we can use a
            // simpler routine to write the BigInteger value.
            if (bigInt.compareTo(MAX_LONG_VALUE) < 0)
            {
                long lvalue = bigInt.longValue();
                writeUInt(lvalue);
            }
            else
            {
                // BigInteger.toByteArray() returns a two's complement
                // representation byte array. However, we have negated all
                // negative BigInts to become positive BigInts, so essentially
                // we don't have to convert the two's complement representation
                // to sign-magnitude UInt.
                byte[] bits = bigInt.toByteArray();

                // BigInteger will pad this with a null byte sometimes
                // for negative numbers. Let's skip past any leading null bytes.
                int offset = 0;
                while (offset < bits.length && bits[offset] == 0)
                {
                    offset++;
                }

                int actualBitLength = bits.length - offset;
                int bufferOffset = myOffset - actualBitLength;
                if (bufferOffset < 0) {
                    bufferOffset = growBuffer(bufferOffset);
                }
                System.arraycopy(bits, offset, myBuffer, bufferOffset,
                                 actualBitLength);
                myOffset = bufferOffset;
            }

            writePrefix(type, myBuffer.length - myOffset - originalOffset);
        }
    }

    private void writeIonFloatContent(IonFloat val)
    {
        if (val.isNullValue())
        {
            writeByte((byte) (TYPE_FLOAT | NULL_LENGTH_MASK));
        }
        else
        {
            // Write a 64-bit value in IEE-754 standard. This format happens to
            // match the 8-byte UInt encoding.
            long bits = Double.doubleToRawLongBits(val.doubleValue());
            int offset = myOffset;
            if ((offset -= 8) < 0) {
                offset = growBuffer(offset);
            }

            myBuffer[offset]     = (byte) (bits >>> (8 * 7));
            myBuffer[offset + 1] = (byte) (bits >>> (8 * 6));
            myBuffer[offset + 2] = (byte) (bits >>> (8 * 5));
            myBuffer[offset + 3] = (byte) (bits >>> (8 * 4));
            myBuffer[offset + 4] = (byte) (bits >>> (8 * 3));
            myBuffer[offset + 5] = (byte) (bits >>> (8 * 2));
            myBuffer[offset + 6] = (byte) (bits >>> (8 * 1));
            myBuffer[offset + 7] = (byte)  bits;

            myOffset = offset;

            writePrefix(TYPE_FLOAT, 8); // 64-bit IEE-754
        }
    }

    private static final byte[] negativeZeroBitArray = new byte[] { (byte) 0x80 };
    private static final byte[] positiveZeroBitArray = new byte[0];

    /**
     * @see software.amazon.ion.impl.IonBinary.Writer#writeDecimalContent
     */
    private void writeIonDecimalContent(BigDecimal bd)
    {
        BigInteger mantissa = bd.unscaledValue();

        byte[] mantissaBits;

        switch (mantissa.signum())
        {
            case 0:
                if (Decimal.isNegativeZero(bd))
                {
                    mantissaBits = negativeZeroBitArray;
                }
                else
                {
                    mantissaBits = positiveZeroBitArray;
                }
                break;
            case -1:
                // Obtain the unsigned value of the BigInteger
                // We cannot use the twos complement representation of a
                // negative BigInteger as this is different from the encoding
                // of basic field Int.
                mantissaBits = mantissa.negate().toByteArray();
                // Set the sign on the highest order bit of the first octet
                mantissaBits[0] |= 0x80;
                break;
            case 1:
                mantissaBits = mantissa.toByteArray();
                break;
            default:
                throw new IllegalStateException("mantissa signum out of range");
        }

        writeBytes(mantissaBits);

        // Ion stores exponent, BigDecimal uses the negation 'scale' instead
        int exponent = -bd.scale();
        writeVarInt(exponent);
    }

    private void writeIonDecimalContent(IonDecimal val)
    {
        if (val.isNullValue())
        {
            writeByte((byte) (TYPE_DECIMAL | NULL_LENGTH_MASK));
        }
        else
        {
            final int originalOffset = myBuffer.length - myOffset;
            writeIonDecimalContent(val.decimalValue());
            writePrefix(TYPE_DECIMAL,
                        myBuffer.length - myOffset - originalOffset);
        }
    }

    private void writeIonTimestampContent(IonTimestamp val)
    {
        if (val.isNullValue())
        {
            writeByte((byte) (TYPE_TIMESTAMP | NULL_LENGTH_MASK));
        }
        else
        {
            final int originalOffset = myBuffer.length - myOffset;

            Timestamp t = val.timestampValue();

            // Time and date portion
            switch (t.getPrecision())
            {
                // Fall through each case - by design
                case SECOND:
                {
                    BigDecimal fraction = t.getZFractionalSecond();
                    if (fraction != null)
                    {
                        assert (fraction.signum() >= 0
                                && ! fraction.equals(BigDecimal.ZERO))
                            : "Bad timestamp fraction: " + fraction;
                        writeIonDecimalContent(fraction);
                    }
                    writeVarUInt(t.getZSecond());
                }
                case MINUTE:
                    writeVarUInt(t.getZMinute());
                    writeVarUInt(t.getZHour());
                case DAY:
                    writeVarUInt(t.getZDay());
                case MONTH:
                    writeVarUInt(t.getZMonth());
                case YEAR:
                    writeVarUInt(t.getZYear());
                    break;
                default:
                    throw new IllegalStateException(
                              "unrecognized Timestamp precision: " +
                              t.getPrecision());
            }

            // Offset portion
            Integer offset = t.getLocalOffset();
            if (offset == null)
            {
                writeByte((byte) (0x80 | 0x40)); // Negative 0 (no timezone)
            }
            else
            {
                writeVarInt(offset.intValue());
            }

            writePrefix(TYPE_TIMESTAMP,
                        myBuffer.length - myOffset - originalOffset);
        }
    }

    private void writeIonSymbolContent(IonSymbol val)
    {
        if (val.isNullValue())
        {
            writeByte((byte) (TYPE_SYMBOL | NULL_LENGTH_MASK));
        }
        else
        {
            final int originalOffset = myBuffer.length - myOffset;
            SymbolToken symToken = val.symbolValue();
            int sid = findSid(symToken);
            writeUInt(sid);

            writePrefix(TYPE_SYMBOL,
                        myBuffer.length - myOffset - originalOffset);
        }
    }

    private void writeIonStringContent(IonString val)
    {
        if (val.isNullValue())
        {
            writeByte((byte) (TYPE_STRING | NULL_LENGTH_MASK));
        }
        else
        {
            writeIonStringContent(val.stringValue());
        }
    }

    private void writeIonStringContent(String str)
    {
        int strlen = str.length();
        byte[] buffer = myBuffer;
        int offset = myOffset;

        // The number of UTF-8 code units (bytes) we will write is at least as
        // large as the number of UTF-16 code units (ints) that are in the
        // input string.  Ensure we have at least that much capacity, to reduce
        // the number of times we need to grow the buffer.
        offset -= strlen;
        if (offset < 0)
        {
            offset = growBuffer(offset);
            buffer = myBuffer;
        }
        offset += strlen;

        // Optimize for ASCII, under the assumption that it happens a lot.
        // This fits within the capacity allocated above, so we don't have to
        // grow the buffer within this loop.
        int i = strlen - 1;
        for (; i >= 0; --i)
        {
            int c = str.charAt(i);
            if (!(c <= 0x7f))
                break;
            buffer[--offset] = (byte) c;
        }

        for (; i >= 0; --i)
        {
            int c = str.charAt(i);

            if (c <= 0x7f)              // U+0000 to U+007f codepoints
            {
                if (--offset < 0)
                {
                    offset = growBuffer(offset);
                    buffer = myBuffer;
                }
                buffer[offset] = (byte) c;
            }
            else if (c <= 0x7ff)        // U+0080 to U+07ff codepoints
            {
                if ((offset -= 2) < 0)
                {
                    offset = growBuffer(offset);
                    buffer = myBuffer;
                }
                buffer[offset]     = (byte) (0xc0 | ((c >> 6) & 0x1f));
                buffer[offset + 1] = (byte) (0x80 | (c & 0x3f));
            }
            else if (c >= 0xd800 && c <= 0xdfff) // Surrogate!
            {
                // high surrogate not followed by low surrogate
                if (c <= 0xdbff)
                {
                    throw new IonException("invalid string, unpaired high surrogate character");
                }

                // string starts with low surrogate
                if (i == 0)
                {
                    throw new IonException("invalid string, unpaired low surrogate character");
                }

                // low surrogate not preceded by high surrogate
                // charAt(--i) is never out of bounds as i == 0 is asserted to
                // be false in previous if-block
                int c2 = str.charAt(--i);
                if (!(c2 >= 0xd800 && c2 <= 0xdbff))
                {
                    throw new IonException("invalid string, unpaired low surrogate character");
                }

                // valid surrogate pair: (c2, c)
                int codepoint = 0x10000 + (((c2 & 0x3ff) << 10) | (c & 0x3ff));

                if ((offset -= 4) < 0)
                {
                    offset = growBuffer(offset);
                    buffer = myBuffer;
                }
                buffer[offset]     = (byte) (0xF0 | ((codepoint >> 18) & 0x07));
                buffer[offset + 1] = (byte) (0x80 | ((codepoint >> 12) & 0x3F));
                buffer[offset + 2] = (byte) (0x80 | ((codepoint >> 6)  & 0x3F));
                buffer[offset + 3] = (byte) (0x80 | ((codepoint >> 0)  & 0x3F));
            }
            else // U+0800 to U+D7FF and U+E000 to U+FFFF codepoints
            {
                if ((offset -= 3) < 0)
                {
                    offset = growBuffer(offset);
                    buffer = myBuffer;
                }
                buffer[offset]     = (byte) (0xE0 | ((c >> 12) & 0x0F));
                buffer[offset + 1] = (byte) (0x80 | ((c >> 6) & 0x3F));
                buffer[offset + 2] = (byte) (0x80 | (c & 0x3F));
            }
        }

        int length = myOffset - offset;
        myOffset = offset;

        writePrefix(TYPE_STRING, length);
    }

    private void writeIonClobContent(IonClob val)
    {
        if (val.isNullValue())
        {
            writeByte((byte) (TYPE_CLOB | NULL_LENGTH_MASK));
        }
        else
        {
            byte[] lob = val.getBytes();
            writeLobContent(lob);
            writePrefix(TYPE_CLOB, lob.length);
        }
    }

    private void writeIonBlobContent(IonBlob val)
    {
        if (val.isNullValue())
        {
            writeByte((byte) (TYPE_BLOB | NULL_LENGTH_MASK));
        }
        else
        {
            byte[] lob = val.getBytes();
            writeLobContent(lob);
            writePrefix(TYPE_BLOB, lob.length);
        }
    }

    private void writeLobContent(byte[] lob)
    {
        int length = lob.length;
        int offset = myOffset - length;
        if (offset < 0) {
            offset = growBuffer(offset);
        }
        System.arraycopy(lob, 0, myBuffer, offset, length);
        myOffset = offset;
    }

    // =========================================================================
    // Containers
    // =========================================================================

    private void writeIonListContent(IonList val)
    {
        if (val.isNullValue())
        {
            writeByte((byte) (TYPE_LIST | NULL_LENGTH_MASK));
        }
        else
        {
            writeIonSequenceContent(val);
        }
    }

    private void writeIonSexpContent(IonSexp val)
    {
        if (val.isNullValue())
        {
            writeByte((byte) (TYPE_SEXP | NULL_LENGTH_MASK));
        }
        else
        {
            writeIonSequenceContent(val);
        }
    }

    private void writeIonSequenceContent(IonSequence seq)
    {
        final int originalOffset = myBuffer.length - myOffset;
        IonValue[] values = seq.toArray();

        for (int i = values.length; --i >= 0;)
        {
            writeIonValue(values[i]);
        }

        switch (seq.getType())
        {
            case LIST:
                writePrefix(TYPE_LIST,
                            myBuffer.length - myOffset - originalOffset);
                break;
            case SEXP:
                writePrefix(TYPE_SEXP,
                            myBuffer.length - myOffset - originalOffset);
                break;
            default:
                throw new IonException(
                          "cannot identify instance of IonSequence");
        }
    }

    private void writeIonStructContent(IonStruct val)
    {
        if (val.isNullValue())
        {
            writeByte((byte) (TYPE_STRUCT | NULL_LENGTH_MASK));
        }
        else
        {
            final int originalOffset = myBuffer.length - myOffset;

            // TODO amznlabs/ion-java#31 should not preserve the ordering of fields
            ArrayList<IonValue> values = new ArrayList<IonValue>();

            // Fill ArrayList with IonValues, the add() just copies the
            // references of the IonValues
            for (IonValue curr : val)
            {
                values.add(curr);
            }

            for (int i = values.size(); --i >= 0; )
            {
                IonValue v = values.get(i);
                SymbolToken symToken = v.getFieldNameSymbol();

                writeIonValue(v);

                int sid = findSid(symToken);
                writeVarUInt(sid);
            }

            // TODO amznlabs/ion-java#41 Detect if the struct fields are sorted in ascending
            // order of Sids. If so, 1 should be written into 'length' field.
            // Note that this 'length' field is not the same as the four-bit
            // length L in the type descriptor octet.
            writePrefix(TYPE_STRUCT,
                        myBuffer.length - myOffset - originalOffset);
        }
    }

    private void writeIonDatagramContent(IonDatagram dg)
    {
        ListIterator<IonValue> reverseIter = dg.listIterator(dg.size());
        while (reverseIter.hasPrevious())
        {
            IonValue currentTopLevelValue = reverseIter.previous();
            checkLocalSymbolTablePlacement(currentTopLevelValue);
            writeIonValue(currentTopLevelValue);
        }
    }

    // =========================================================================
    // Symbol Tables
    // =========================================================================

    private int findSid(SymbolToken symToken)
    {
        int sid = symToken.getSid();
        String text = symToken.getText();

        if (sid != UNKNOWN_SYMBOL_ID)   // sid is assigned
        {
            assert text == null ||
                   text.equals(mySymbolTable.findKnownSymbol(sid));
        }
        else                            // sid is not assigned
        {
            if (mySymbolTable.isSystemTable())
            {
                // Replace current symtab with a new local symbol table
                // using the default system symtab
                mySymbolTable = myIonSystem.newLocalSymbolTable();
            }

            // Intern the new symbol and get its assigned sid
            sid = mySymbolTable.intern(text).getSid();
        }

        return sid;
    }

    /**
     * Determine if the local symbol table attached to the previous top-level
     * value (TLV), {@link #mySymbolTable}, needs to be encoded before the
     * next TLV is encoded. This is called <em>before</em> encoding each
     * TLV by {@link #writeIonValue(IonValue)}.
     * <p>
     * Connotations of "Previous TLV" and "Next TLV" in this method are
     * <em>different</em> from those defined outside of this method.
     * This is done on purpose within this method to facilitate a clear
     * understanding of what is going on within this method.
     * <ul>
     *    <li>"Previous top-level value" refers to the top-level IonValue that
     *    has already been encoded into the buffer.
     *    <li>"Next top-level value" refers to the top-level IonValue that
     *    is about to be encoded to the buffer. Its contents are <em>not</em>
     *    traversed yet.
     *    <li>"Previous symbol table" refers to previous TLV's symbol table.
     *    <li>"Next symbol table" refers to next TLV's symbol table.
     * </ul>
     *
     * Local symbol tables and IVMs can be interspersed within an IonDatagram.
     * This method checks for such cases by looking at the next symtab and
     * previous symtab.
     *
     * <h2>The following 4 cases define the scenarios where a LST/IVM is
     * written to the buffer:</h2>
     * <p>
     * Next symtab is a local table:
     * <ul>
     *    <li>Previous symtab is a local table - write LST if the two symtabs
     *    are different references
     *    <li>Previous symtab is a system table - write IVM always
     * </ul>
     * <p>
     * Next symtab is a system table:
     * <ul>
     *    <li>Previous symtab is a local table - propagate LST upwards
     *    <li>Previous symtab is a system table - write IVM if the two symtabs
     *    have different Ion versions.
     * </ul>
     *
     * TODO amznlabs/ion-java#25 Currently, {@link IonDatagram#systemIterator()} doesn't
     * retain information about interspersed IVMs within the IonDatagram.
     * As such, we cannot obtain the location of interspersed IVMs, if any.
     *
     * @param nextTopLevelValue the next top-level IonValue to be encoded
     */
    private void checkLocalSymbolTablePlacement(IonValue nextTopLevelValue)
    {
        // Check that nextTopLevelValue is indeed a top-level value
        assert nextTopLevelValue == nextTopLevelValue.topLevelValue();

        SymbolTable nextSymTab = nextTopLevelValue.getSymbolTable();

        if (nextSymTab == null) {
            throw new IllegalStateException(
                      "Binary reverse encoder isn't using LiteImpl");
        }

        if (mySymbolTable == null) {
            // There is no current symtab, i.e. there wasn't any TLV encoded
            // before this, return and continue encoding next TLV.
            mySymbolTable = nextSymTab;
            return;
        }

        assert nextSymTab.isLocalTable() || nextSymTab.isSystemTable();

        if (nextSymTab.isLocalTable())
        {
            if (mySymbolTable.isSystemTable())
            {
                writeBytes(BINARY_VERSION_MARKER_1_0);
                mySymbolTable = nextSymTab;
            }
            // mySymbolTable is local
            else if (nextSymTab != mySymbolTable)
            {
                writeLocalSymbolTable(mySymbolTable);
                mySymbolTable = nextSymTab;
            }
        }
        // nextSymTab is system
        else if (mySymbolTable.isSystemTable() &&
                !mySymbolTable.getIonVersionId().equals(nextSymTab.getIonVersionId()))
        {
            writeBytes(BINARY_VERSION_MARKER_1_0);
            mySymbolTable = nextSymTab;
        }
    }

    /**
     * Write contents of a local symbol table as a struct.
     * The contents are the IST:: annotation, declared symbols, and import
     * declarations (that refer to shared symtabs) if they exist.
     *
     * @param symTab the local symbol table, not shared, not system
     */
    private void writeLocalSymbolTable(SymbolTable symTab)
    {
        assert symTab.isLocalTable();

        final int originalOffset = myBuffer.length - myOffset;

        // Write declared local symbol strings if any exists
        writeSymbolsField(symTab);

        // Write import declarations if any exists
        writeImportsField(symTab);

        // Write the struct prefix
        writePrefix(TYPE_STRUCT, myBuffer.length - myOffset - originalOffset);

        // Write the $ion_symbol_table annotation
        byte[] ionSymbolTableByteArray = {
               (byte) (0x80 | 1),                       /* annot-length */
               (byte) (0x80 | ION_SYMBOL_TABLE_SID)     /* annot */
               };
        writeBytes(ionSymbolTableByteArray);
        writePrefix(TYPE_ANNOTATIONS,
                    myBuffer.length - myOffset - originalOffset);
    }

    /**
     * Write a single import declaration (which refers to a shared SymbolTable).
     *
     * @param symTab the shared symbol table, not local, not system
     */
    private void writeImport(SymbolTable symTab)
    {
        assert symTab.isSharedTable();

        final int originalOffset = myBuffer.length - myOffset;

        // Write the maxId as int
        int maxId = symTab.getMaxId();
        if (maxId == 0) {
            writeByte((byte) TYPE_POS_INT);
        } else {
            writeUInt(maxId);
            writePrefix(TYPE_POS_INT,
                        myBuffer.length - myOffset - originalOffset);
        }

        // Write the "max_id" field name
        writeByte((byte) (0x80 | MAX_ID_SID));

        final int maxIdOffset = myBuffer.length - myOffset;

        // Write the version as int (version will be at least one)
        int version = symTab.getVersion();
        writeUInt(version);
        writePrefix(TYPE_POS_INT, myBuffer.length - myOffset - maxIdOffset);

        // Write the "version" field name
        writeByte((byte) (0x80 | VERSION_SID));

        // Write the name as string
        String name = symTab.getName();
        writeIonStringContent(name);

        // Write the "name" field name
        writeByte((byte) (0x80 | NAME_SID));

        // Write the struct prefix
        writePrefix(TYPE_STRUCT, myBuffer.length - myOffset - originalOffset);
    }

    /**
     * Write import declarations (which refer to shared symbol tables) if any
     * exists.
     *
     * @param symTab the local symbol table, not shared, not system
     */
    private void writeImportsField(SymbolTable symTab)
    {
        // SymbolTable[] holds accurate information, i.e. it contains the
        // actual import declaration information, through the means of
        // substitute tables if an exact match was not found by the catalog.
        SymbolTable[] sharedSymTabs = symTab.getImportedTables();

        if (sharedSymTabs.length == 0)
        {
            return;
        }

        final int importsOffset = myBuffer.length - myOffset;

        for (int i = sharedSymTabs.length; --i >= 0;)
        {
            writeImport(sharedSymTabs[i]);
        }

        writePrefix(TYPE_LIST, myBuffer.length - myOffset - importsOffset);
        writeByte((byte) (0x80 | IMPORTS_SID));
    }

    /**
     * Write declared local symbol names if any exists.
     *
     * @param symTab the local symbol table, not shared, not system
     */
    private void writeSymbolsField(SymbolTable symTab)
    {
        // SymbolTable's APIs doesn't expose an Iterator to traverse declared
        // symbol strings in reverse order. As such, we utilize these two
        // indexes to traverse the strings in reverse.
        int importedMaxId = symTab.getImportedMaxId();
        int maxId = symTab.getMaxId();

        if (importedMaxId == maxId) {
            // There are no declared local symbols
            return;
        }

        final int originalOffset = myBuffer.length - myOffset;

        for (int i = maxId; i > importedMaxId; i--)
        {
            String str = symTab.findKnownSymbol(i);
            if (str == null) {
                writeByte((byte) (TYPE_STRING | NULL_LENGTH_MASK));
            } else {
                writeIonStringContent(str);
            }
        }

        writePrefix(TYPE_LIST, myBuffer.length - myOffset - originalOffset);
        writeByte((byte) (0x80 | SYMBOLS_SID));
    }

}
