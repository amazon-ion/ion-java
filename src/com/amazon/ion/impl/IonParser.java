/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;


import static com.amazon.ion.impl.IonConstants.MAGIC_COOKIE;
import static com.amazon.ion.impl.IonConstants.MAGIC_COOKIE_SIZE;
import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;

import com.amazon.ion.IonException;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.UnexpectedEofException;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.impl.IonBinary.PositionMarker;
import com.amazon.ion.util.Text;


/**
 * The Ion text to binary transcoder.
 */
public class IonParser
{

    private IonTokenReader      _in;
    private BufferManager       _out;
    private LocalSymbolTable    _symboltable;
    private IonTokenReader.Type _t;
    private ArrayList<Integer>  _annotationList;


    /**
     * @param bb may be null, in which case a new {@link BufferManager} will
     * be created.
     *
     * @throws NullPointerException if r is null.
     */
    public IonParser(Reader r, BufferManager bb) {
        if (r == null) throw new NullPointerException();

        _in = new IonTokenReader(r);
        if (bb != null) {
            _out = bb;
        }
        else {
            _out = new BufferManager();
        }
    }

    /**
     * @throws NullPointerException if r is null.
     */
    public IonParser(Reader r) {
        this(r, new BufferManager());
    }
    public IonParser(String s) {
        this(new StringReader(s), new BufferManager());
    }

    public BufferManager getByteBuffer() {
        return this._out;
    }
    public byte[] getBytes() {
        int    len = this._out.buffer().size();
        byte[] bytes = new byte[len];
        IonBinary.Reader reader;
        try {
            reader = _out.reader(0);
            reader.read(bytes, 0, len);
        }
        catch (IOException e) {
            throw new IonException(e);
        }

        return bytes;
    }

    final IonTokenReader.Type next(boolean is_in_expression) throws IOException {
        return (_t = this._in.next(is_in_expression));
    }


    /**
     *
     * @param symboltable must not be null.
     * @param startPosition
     *      The position to start writing into our buffer.
     * @param writeMagicCookie
     *      Whether we should write the Ion magic cookie before any data.
     * @param consume the preferred number of characters to consume.  More than
     * this number may be read, but we won't stop until we pass this threshold.
     */
    public void parse(LocalSymbolTable symboltable
                    , int startPosition
                    , boolean writeMagicCookie
                    , int consume)
    {
        assert symboltable != null;
        this._symboltable = symboltable;

        // now start the hard work, we catch all the IOExceptions here
        // so that we don't have to try/catch all the time, or throw them
        try {
            _out.openReader();
            _out.openWriter();
            IonBinary.Writer writer = _out.writer(startPosition);

            if (writeMagicCookie) {
                // Start the buffer with the magic cookie.
                writer.writeFixedIntValue(MAGIC_COOKIE, MAGIC_COOKIE_SIZE);
            }

            do {
                // get the first token
                next(true);
                if (this._t == IonTokenReader.Type.eof) {
                    break;
                }
                // we are an annotated value here
                parseAnnotatedValue(true);

                //  FIXME need symtab logic here!!!

            } while (consume > this._in.getConsumedAmount());

            // and we're done
            this._out.writer().truncate();
        }
        catch (IOException ioe) {
            throw new IonException(ioe.getMessage()+ " at " + this._in.position(),
                                    ioe);
        }
    }

    void parseAnnotatedValue(boolean is_in_expression) throws IOException {
        boolean is_annotated = false;

        // if it's a user type decl (id::) start the annotation
        if (this._t == IonTokenReader.Type.constUserTypeDecl) {
            this.startAnnotations();
            is_annotated = true;

            while (this._t == IonTokenReader.Type.constUserTypeDecl)
            {
                String annotation = this._in.getValueString(is_in_expression);
                this.addAnnotation(annotation);
                next(is_in_expression);
            }
        }

        // now we expect just one plain value
        parsePlainValue(is_in_expression);

        // if it was annotated, close out the annotation
        if (is_annotated) {
            this.closeAnnotations();
        }
    }

    void startAnnotations() throws IOException
    {
        _annotationList = new ArrayList<Integer>();
        this._out.writer().pushPosition(_annotationList);
        this._out.writer().write(IonConstants.makeTypeDescriptor(IonConstants.tidTypedecl, 0));
        this._out.writer().writeVarInt7Value(1, true); // we'll have at least 1 byte of annotations
        this._out.writer().write((byte)0);       // and here's at least 1 annotation
    }

    void addAnnotation(String annotation)
    {
        if (annotation.length() < 1) {
            throw new IonException("symbols must be at least 1 character long");
        }
        int sid = this._symboltable.addSymbol(annotation);
        assert sid > 0;
        _annotationList.add(new Integer(sid));
    }

    @SuppressWarnings("unchecked")
    void closeAnnotations() throws IOException
    {
        PositionMarker pm = this._out.writer().popPosition();
        int startPos = pm.getPosition();
        Object o = pm.getUserData();
        _annotationList = (ArrayList<Integer>)(o); // <-- @ SuppressWarnings("unchecked")

        // remember in all this that we already have 3 bytes of annotation
        // header written: ths annotation td byte, a 1 for length,
        // and a 0 as a place holder for the first annotation symbol
        int endPos = this._out.writer().position();
        int writtenDataLen = endPos - startPos;

        int annotationLen = IonBinary.lenAnnotationListWithLen(_annotationList);

        // it's (valueLen - 2) because the type desc, then placeholder annotation
        // length (1) and the placeholder 0 symbol we wrote have been inappropriately
        // counted in the valueLen
        int totalAnnotationAndValueLen = annotationLen + (writtenDataLen - 3);

        int tdLen = IonBinary.lenLenFieldWithOptionalNibble(totalAnnotationAndValueLen);
        tdLen += IonConstants.BB_TOKEN_LEN; // and we have the typedesc byte too

        // so we know the annotation td and len is tdLen, the annotation
        // list itself is annotationLen and we already wrote 3 bytes in
        // the steam to get started, how much more do we need?
        // so the -3 here accounts for the td, 1, and 0 we started this off with
        int insertLen = tdLen + annotationLen - 3;
        assert insertLen >= 0;

        // now backup and re-write the header and the annotation list
        this._out.writer().setPosition(startPos);
        this._out.writer().insert(insertLen);

        // the length here is the bytes of annotations + the value we
        // wrote as we parsed it
        this._out.writer().writeCommonHeader(
                                IonConstants.tidTypedecl
                               ,totalAnnotationAndValueLen
                           );
        this._out.writer().writeAnnotations(_annotationList);

        // position the writer back ready for the next value
        this._out.writer().setPosition(endPos + insertLen);
    }

    void parsePlainValue(boolean is_in_expression) throws IOException  {

        switch(_t) {
        case eof:
            break;
        case constNegInt:
        case constPosInt:
        case constFloat:
        case constDecimal:
        case constTime:
            parseCastNumeric(_t);
            break;
        case constString:
            if (this._in.isIncomplete) {
                if (this._in.isLongString) {
                    transferLongString(_t.getHighNibble().value());
                }
                else {
                    transferString(_t.getHighNibble().value());
                }
            }
            else {
                String s = this._in.getValueString(is_in_expression);
                this._out.writer().writeStringWithTD(s);
            }
            break;
        case constSymbol:
            processSymbolValue(is_in_expression);
            // now, move along.  there's nothing more to see here ...
            break;

        case tOpenParen:
            // Houston, we have an sexp
            parseSexpBody( );
            break;
        case tOpenSquare:
            // Houston, we have a list
            parseListBody( );
            break;
        case tOpenCurly:
            // Quit calling me Houston and ... it's a struct
            parseStructBody( );
            break;
        case tOpenDoubleCurly:
            // double curly? who thought double curly was a good idea?
            parseLobContents();
            break;

        default:
            if (_t.isKeyword()) {
                processKeywordValue();
            }
            else {
                throw new IonException("expected value is invalid at "
                                        + this._in.position());
            }
        }
    }

    private void transferString(int hn) throws IOException
    {
        // here we'll "let" the BufferBytes class handle the heavy lifting
        this._out.writer().startLongWrite(hn);

        boolean onlyByteSizedCharacters
           = (hn == IonConstants.tidClob);

        // First copy the characters we've already accummulated.
        this._out.writer().appendToLongValue(_in.value, onlyByteSizedCharacters);
        _in.value.setLength(0);

        // Read the rest of the string
        PushbackReader r = this._in.getPushbackReader();
        this._out.writer().appendToLongValue('\"', false, onlyByteSizedCharacters, r);

        this._out.writer().patchLongHeader(hn, -1);
    }

    private void transferLongString(int hn) throws IOException
    {
        // We don't accumulate any value in the long-string case.
        assert _in.value.length() == 0;

        int c;
        boolean currentInQuotedContentState = this._in.inQuotedContent;

        boolean onlyByteSizedCharacters
           = (hn == IonConstants.tidClob);

        // here we'll "let" the BufferBytes class handle the heavy lifting
        PushbackReader r = this._in.getPushbackReader();
        this._out.writer().startLongWrite(hn);
        this._out.writer().appendToLongValue('\'', this._in.isLongString, onlyByteSizedCharacters, r);

        // Concatenate any long-strings following this one.
        while (this._in.isLongString) {
            this._in.inQuotedContent = false;
            c = this._in.readIgnoreWhitespace();
            this._in.inQuotedContent = currentInQuotedContentState;
            // if we see 3 quotes this string continues
            if (c != '\'') {
                this._in.unread(c);
                break;
            }
            c = this._in.read();
            if (c != '\'') {
                this._in.unread(c);
                this._in.unread('\''); // the one we went by before
                break;
            }
            c = this._in.read();
            if (c != '\'') {
                this._in.unread(c);
                this._in.unread('\''); // the one we went by before
                this._in.unread('\''); // and the first one as well
                break;
            }
            this._out.writer().appendToLongValue('\'', true, onlyByteSizedCharacters, r);
        }
        this._out.writer().patchLongHeader(hn, -1);
    }

    void parseSexpBody( ) throws IOException  {

        assert _t == IonTokenReader.Type.tOpenParen;

        _out.writer().startLongWrite(IonConstants.tidSexp);

loop:   for (;;) {
            next(true);
            switch(_t) {
            case eof:
            case tCloseParen:
                // EOF breaks loop, but we verify close-paren below.
                break loop;
            }
            // get the value
            parseAnnotatedValue( true );
        }

        if (_t != IonTokenReader.Type.tCloseParen) {
            throw new IonException("this list needs a closing paren at " + this._in.position());
        }

        // here we backpatch the head of this list
        // TODO shouldn't need to pass high-nibble again.
        // lownibble is ignored and computed from amount written.
        this._out.writer().patchLongHeader(IonConstants.tidSexp, 0);
    }

    void parseListBody( ) throws IOException  {

        assert _t == IonTokenReader.Type.tOpenSquare;

        _out.writer().startLongWrite(IonConstants.tidList);

loop:   for (;;) {
            next(false);
            switch(_t) {
            case eof:
            case tCloseSquare:
                // EOF breaks loop, but we verify ']' below.
                break loop;
            }
            // get the value
            parseAnnotatedValue(false);
            next(false);

            if (_t != IonTokenReader.Type.tComma) break;

        }
        if (_t != IonTokenReader.Type.tCloseSquare) {
            throw new IonException("expected ',' or ']' in list at " + this._in.position());
        }

        // here we backpatch the head of this list
        // lownibble is ignored and computed from amount written.
        this._out.writer().patchLongHeader(IonConstants.tidList, 0);
    }

    void parseStructBody(  ) throws IOException  {
        boolean string_name = false;

        assert _t == IonTokenReader.Type.tOpenCurly;

        int hn = IonConstants.tidStruct;
        int ln = IonConstants.lnNumericZero;  // TODO justify
        _out.writer().pushLongHeader(hn, ln, true);
        this._out.writer().writeStubStructHeader(hn, ln);

        // read string tagged values - legal tags will be recognized
        this._in.pushContext(IonTokenReader.Context.STRUCT);

        // now, step over the curly brace
        next(false);

//          while(_t.isTag()) {
loop:   for (;;) {
            switch(_t) {
            case eof:
            case tCloseCurly:
                // EOF breaks loop, but we verify close-curly below.
                break loop;
            case constMemberName:
                string_name = false;
                break;
            case constString:
                string_name = true;
                break;
            default:
                throw new IonException("missing structure member name at " + this._in.position());
            }

            // process the field name
            processFieldName();
            if (string_name) { // TODO why treat string separately from symbol?
                // this._in.inContent = false;
                int c = this._in.readIgnoreWhitespace();
                if (c == ':') {
                    c = this._in.read();
                    if (c == ':') {
                        throw new IonException("member name expected but usertypedesc found at " + this._in.position());
                    }
                    this._in.unread(c);
                }
            }
            next(false);

            // now read the value (which might be annotated)
            parseAnnotatedValue(false);
            next(false);

            switch(_t) {
            case eof:
            case tCloseCurly:
                // EOF breaks loop, but we verify close-curly below.
                break loop;
            case tComma:
                break;
            default:
                throw new IonException("expected ',' or '}' in struct at " + this._in.position());
            }

            // we're past that comma
            next(false);
        }

        if (_t != IonTokenReader.Type.tCloseCurly) {
            throw new IonException("this structure needs a closing curly brace at " + this._in.position());
        }

        // TODO check for "accidental" ordering.
        boolean fieldsAreOrdered = false;

        // TODO This code path is WAY ugly and needs cleanup.
        int lowNibble = (fieldsAreOrdered ? IonConstants.lnIsOrderedStruct : 0);
        this._out.writer().patchLongHeader(IonConstants.tidStruct,
                                  lowNibble);
        this._in.popContext();
    }

    void processFieldName() throws IOException {
        // otherwise it's just a symbol - write it out the hard way
        String s = this._in.getValueString(/* is_in_expression */ false);
        if (s.length() < 1) {
            throw new IonException("symbols must be at least 1 character long");
        }

        int sid = this._symboltable.addSymbol(s);
        this._out.writer().writeVarUInt7Value(sid, true);
    }

    /**
     * Encodes symbols (does not include keywords like true/false/null*).
     *
     * @param is_in_expression
     * @throws IOException
     */
    void processSymbolValue(boolean is_in_expression) throws IOException
    {
        assert this._in.keyword == null;

        String s = this._in.getValueString(is_in_expression);
        if (s.length() < 1) {
            throw new IonException("symbols must be at least 1 character long");
        }
        this._out.writer().writeSymbolWithTD(s, _symboltable);
    }

    void processKeywordValue() throws IOException
    {
        assert this._in.keyword != null;

        int hn = this._in.keyword.getHighNibble().value();

        int token;
        switch (this._in.keyword) {
            case kwTrue:
                token = IonConstants.True;
                break;
            case kwFalse:
                token = IonConstants.False;
                break;
            case kwNull:
            case kwNullNull:
            case kwNullBoolean:
            case kwNullInt:
            case kwNullFloat:
            case kwNullDecimal:
            case kwNullTimestamp:
            case kwNullSymbol:
            case kwNullString:
            case kwNullBlob:
            case kwNullClob:
            case kwNullList:
            case kwNullSexp:
                token = IonConstants.makeTypeDescriptor(hn,
                                         IonConstants.lnIsNullAtom);
                break;
            case kwNullStruct:
                token = IonConstants.makeTypeDescriptor(
                                         hn,
                                         IonConstants.lnIsNullStruct);
                break;
            default:
                throw new IllegalStateException("bad keyword token");
        }

        this._out.writer().write(token);
    }

    void parseCastNumeric(IonTokenReader.Type castto) throws IOException {
        _in.makeValidNumeric(castto);
        switch (castto) {
        case constPosInt:
            {
                long l = this._in.intValue.longValue();
                int size = IonBinary.lenVarUInt8(l);
                this._out.writer().writeByte(
                        castto.getHighNibble(),
                        size);
                this._out.writer().writeVarUInt8Value(l, size);
            }
            break;
        case constNegInt:
            {
                long l = 0 - this._in.intValue.longValue();
                int size = IonBinary.lenVarUInt8(l);
                this._out.writer().writeByte(
                        castto.getHighNibble(),
                        size);
                this._out.writer().writeVarUInt8Value(l, size);
            }
        break;
        case constFloat:
            {
                double d = this._in.doubleValue.doubleValue();
                int len = IonBinary.lenIonFloat(d);
                this._out.writer().writeByte(castto.getHighNibble(), (byte)len);
                this._out.writer().writeFloatValue(d);
            }
            break;
        case constDecimal:
            {
                BigDecimal bd = this._in.decimalValue;
                this._out.writer().writeDecimalWithTD(bd);
            }
            break;
        case constTime:
            {
                Date d = this._in.dateValue.d;
                Integer tz = this._in.dateValue.localOffset;
                IonTokenReader.Type.timeinfo di =
                    new IonTokenReader.Type.timeinfo(d, tz);
                this._out.writer().writeTimestampWithTD(di);
            }
            break;
        default:
            throw new IonException("internal error, unrecognized numeric token type at " + this._in.position());
        }

    }

    void parseLobContents( ) throws IOException
    {
        // so we ran into the opening "{{", what we need to do now
        // is peek ahead and find out if we have a quote or not
        int c = this._in.readIgnoreWhitespaceButNotComments();
        this._in.resetValue();

        // if it's a quote we have a "clob"  (clob)
        if (c == '\"' || c == '\'') {
            if (c == '\'') { // that's 1
                c = this._in.read();
                if (c != '\'') {
                    throw new IonException("invalid clob or blob value");
                }
                // that's 2
                c = this._in.read();
                if (c != '\'') {
                    throw new IonException("invalid clob or blob value");
                }
                // that's 3 - it's a long string
                this._in.isLongString = true;
                transferLongString(IonConstants.tidClob);
                this._in.isLongString = false;
            }
            else {
                // somebody else gets to do most of our work here
                transferString(IonConstants.tidClob);
            }

            // we haven't seen the close curly first one yet
            // we finished the "string" at the ending quote
            c = this._in.readIgnoreWhitespaceButNotComments();
            if (c != '}') {
                throw new IonException("invalid clob value, double curly braces expected at " + this._in.position());
            }
            // we saw 1 close curly, is there a second (or should be)
            c = this._in.read();
            if (c != '}') {
                throw new IonException("invalid clob value, double curly braces expected at " + this._in.position());
            }
        }
        else {
            // otherwise we expect base64 encoded text
            // read the base 64 characters and create a binary output value
            // NOTE that we have just one "pushed back" character to deal with
            //      as we just decided the next character wasn't a quote
            PushbackReader innerReader = _in.getPushbackReader();
            innerReader.unread(c); // we just overread the first character looking for a quote (which we didn't fine)
            Base64Encoder.BinaryStream bin64reader
                = new Base64Encoder.BinaryStream(innerReader, '}');
            PushbackReader pbr = new PushbackReader(bin64reader);

            int start = this._out.writer().position();

            _out.writer().startLongWrite(IonConstants.tidBlob);
            _out.writer().appendToLongValue(-1, false, true, pbr);

            // we'll have exited from the reader either on trailing whitespace or
            // the first closing curly brace (the reader will consume the correct
            // number of trailing '=' characters), so skip past any whitespace
            c = bin64reader.terminatingChar();
            while (Text.isWhitespace(c)) {
                c = this._in.readIgnoreWhitespaceButNotComments();
            }
            // we haven't seen the first one yet
            if (c != '}') {
                if (c == -1) {
                    throw new UnexpectedEofException();
                }
                throw new IonException("invalid base64 ending, at least one curly brace was expected at " + this._in.position());
            }
            // we saw 1 close curly, is there a second
            c = this._in.read();
            if (c != '}') {
                if (c == -1) {
                    throw new UnexpectedEofException();
                }
                throw new IonException("invalid base64 ending, double curly braces expected at " + this._in.position());
            }

            int len = this._out.writer().position() - start;
            if (len > IonConstants.lnIsVarLen) {
                len = IonConstants.lnIsVarLen;
            }
            this._out.writer().patchLongHeader(IonConstants.tidBlob, len);
        }
    }

}
