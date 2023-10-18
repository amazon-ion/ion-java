package com.amazon.ion.impl.bin;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;

/**
 * An IonWriter that delegates to an IonRawBinaryWriter, intercepting and attempting to resolve any SymbolTokens
 * in the system symbol table. This allows for system values to be translated verbatim from text to binary.
 */
public class SystemSymbolResolvingIonRawBinaryWriter extends AbstractIonWriter {
    private final IonRawBinaryWriter delegate;
    private final SymbolTable systemSymbolTable;

    public SystemSymbolResolvingIonRawBinaryWriter(_Private_IonRawWriter delegate) {
        super(WriteValueOptimization.NONE);
        this.delegate = (IonRawBinaryWriter) delegate;
        systemSymbolTable = delegate.getSymbolTable().getSystemSymbolTable();
    }

    /**
     * Attempts to resolve a SymbolToken, if necessary, using the system symbol table. If the given token already has
     * a defined symbol ID, then no resolution is required. Otherwise, attempts to look up the given token's text
     * in the system symbol table. If no match is found, then an error is raised, as a symbol token without a symbol ID
     * cannot be represented in binary Ion 1.0.
     * @param token a SymbolToken with defined text.
     * @return a SymbolToken with defined symbol ID.
     */
    private SymbolToken resolve(SymbolToken token) {
        if (token.getSid() > UNKNOWN_SYMBOL_ID) {
            return token;
        }
        SymbolToken resolved = systemSymbolTable.find(token.getText());
        if (resolved == null) {
            throw new IllegalStateException("Cannot write a symbol token without a SID in binary Ion 1.0.");
        }
        return resolved;
    }

    @Override
    public void setFieldNameSymbol(final SymbolToken name) {
        delegate.setFieldNameSymbol(resolve(name));
    }

    @Override
    public void setTypeAnnotationSymbols(final SymbolToken... annotations) {
        SymbolToken[] resolved = new SymbolToken[annotations.length];
        int i = 0;
        for (SymbolToken annotation : annotations) {
            resolved[i++] = resolve(annotation);
        }
        delegate.setTypeAnnotationSymbols(resolved);
    }
    @Override
    public void writeSymbolToken(final SymbolToken content) throws IOException {
        SymbolToken token = resolve(content);
        delegate.writeSymbolToken(token);
    }

    /* ---- Everything that follows simply calls through to the delegate directly. ---- */

    @Override
    public <T> T asFacet(Class<T> facetType) {
        return delegate.asFacet(facetType);
    }

    @Override
    public void writeString(byte[] data, int offset, int length) throws IOException {
        delegate.writeString(data, offset, length);
    }

    @Override
    public SymbolTable getSymbolTable() {
        return delegate.getSymbolTable();
    }

    @Override
    public void setFieldName(final String name) {
        delegate.setFieldName(name);
    }

    @Override
    public void setTypeAnnotations(final String... annotations) {
        delegate.setTypeAnnotations(annotations);
    }

    @Override
    public void addTypeAnnotation(final String annotation) {
        delegate.addTypeAnnotation(annotation);
    }

    @Override
    public IonCatalog getCatalog() {
        return delegate.getCatalog();
    }

    @Override
    public boolean isFieldNameSet() {
        return delegate.isFieldNameSet();
    }

    @Override
    public void writeIonVersionMarker() throws IOException {
        delegate.writeIonVersionMarker();
    }

    @Override
    public int getDepth() {
        return delegate.getDepth();
    }

    @Override
    public void stepIn(final IonType containerType) throws IOException {
        delegate.stepIn(containerType);
    }

    @Override
    public void stepOut() throws IOException {
        delegate.stepOut();
    }

    @Override
    public boolean isInStruct() {
        return delegate.isInStruct();
    }

    @Override
    public void writeNull() throws IOException {
        delegate.writeNull();
    }

    @Override
    public void writeNull(final IonType type) throws IOException {
        delegate.writeNull(type);
    }

    @Override
    public void writeBool(final boolean value) throws IOException {
        delegate.writeBool(value);
    }

    @Override
    public void writeInt(long value) throws IOException {
        delegate.writeInt(value);
    }

    @Override
    public void writeInt(BigInteger value) throws IOException {
        delegate.writeInt(value);
    }

    @Override
    public void writeFloat(final double value) throws IOException {
        delegate.writeFloat(value);
    }

    @Override
    public void writeDecimal(final BigDecimal value) throws IOException {
        delegate.writeDecimal(value);
    }

    @Override
    public void writeTimestamp(final Timestamp value) throws IOException {
        delegate.writeTimestamp(value);
    }

    @Override
    public void writeSymbol(String content) throws IOException {
        delegate.writeSymbol(content);
    }

    @Override
    public void writeString(final String value) throws IOException {
        delegate.writeString(value);
    }

    @Override
    public void writeClob(byte[] data) throws IOException {
        delegate.writeClob(data);
    }

    @Override
    public void writeClob(final byte[] data, final int offset, final int length) throws IOException {
        delegate.writeClob(data, offset, length);
    }

    @Override
    public void writeBlob(byte[] data) throws IOException {
        delegate.writeBlob(data);
    }

    @Override
    public void writeBlob(final byte[] data, final int offset, final int length) throws IOException {
        delegate.writeBlob(data, offset, length);
    }

    @Override
    public void writeBytes(byte[] data, int offset, int length) throws IOException {
        delegate.writeBytes(data, offset, length);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void finish() throws IOException {
        delegate.finish();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
