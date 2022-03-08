package com.amazon.ion.impl.bin;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonMutableCatalog;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.junit.Injected;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

public class IonManagedBinaryWriterTestCase extends IonRawBinaryWriterTest {

    @SuppressWarnings("unchecked")
    private static final List<List<String>> SHARED_SYMBOLS = unmodifiableList(asList(
        unmodifiableList(asList(
            "a",
            "b",
            "c"
        )),
        unmodifiableList(asList(
            "d",
            "e"
        ))
    ));

    private static final Map<String, Integer> SHARED_SYMBOL_LOCAL_SIDS ;
    static
    {
        final Map<String, Integer> sidMap = new HashMap<String, Integer>();

        for (final SymbolToken token : Symbols.systemSymbols())
        {
            sidMap.put(token.getText(), token.getSid());
        }
        int sid = SystemSymbols.ION_1_0_MAX_ID + 1;
        for (final List<String> symbolList : SHARED_SYMBOLS)
        {
            for (final String symbol : symbolList) {
                sidMap.put(symbol, sid);
                sid++;
            }
        }
        SHARED_SYMBOL_LOCAL_SIDS = unmodifiableMap(sidMap);
    }

    protected enum LSTAppendMode
    {
        LST_APPEND_DISABLED,
        LST_APPEND_ENABLED;
        public boolean isEnabled() { return this == LST_APPEND_ENABLED; }
    }

    @Injected.Inject("lstAppendMode")
    public static final LSTAppendMode[] LST_APPEND_ENABLED_DIMENSIONS = LSTAppendMode.values();
    protected LSTAppendMode lstAppendMode;
    public void setLstAppendMode(final LSTAppendMode mode)
    {
        this.lstAppendMode = mode;
    }

    private void checkSymbolTokenAgainstImport(final SymbolToken token)
    {
        final Integer sid = SHARED_SYMBOL_LOCAL_SIDS.get(token.getText());
        if (sid != null)
        {
            assertEquals(sid.intValue(), token.getSid());
        }
    }

    @Override
    protected void additionalValueAssertions(final IonValue value)
    {
        for (final SymbolToken token : value.getTypeAnnotationSymbols()) {
            checkSymbolTokenAgainstImport(token);
        }
        final IonType type = value.getType();
        if (type == IonType.SYMBOL && !value.isNullValue())
        {
            checkSymbolTokenAgainstImport(((IonSymbol) value).symbolValue());
        }
        else if (IonType.isContainer(type))
        {
            for (final IonValue child : ((IonContainer) value))
            {
                additionalValueAssertions(child);
            }
        }
    }

    @Override
    public int ivmLength() {
        return 4;
    }


    @Injected.Inject("importedSymbolResolverMode")
    public static final IonManagedBinaryWriter.ImportedSymbolResolverMode[] RESOLVER_DIMENSIONS = IonManagedBinaryWriter.ImportedSymbolResolverMode.values();

    private IonManagedBinaryWriter.ImportedSymbolResolverMode importedSymbolResolverMode;

    public void setImportedSymbolResolverMode(final IonManagedBinaryWriter.ImportedSymbolResolverMode mode)
    {
        importedSymbolResolverMode = mode;
    }

    @Override
    protected IonWriter createWriter(final OutputStream out) throws IOException
    {
        final IonMutableCatalog catalog = ((IonMutableCatalog) system().getCatalog());

        final List<SymbolTable> symbolTables = new ArrayList<SymbolTable>();
        int i = 1;
        for (final List<String> symbols : SHARED_SYMBOLS) {
            final SymbolTable table = system().newSharedSymbolTable("test_" + (i++), 1, symbols.iterator());
            symbolTables.add(table);
            catalog.putTable(table);
        }

        final _Private_IonManagedBinaryWriterBuilder builder = _Private_IonManagedBinaryWriterBuilder
            .create(_Private_IonManagedBinaryWriterBuilder.AllocatorMode.POOLED)
            .withImports(importedSymbolResolverMode, symbolTables)
            .withPreallocationMode(preallocationMode)
            .withFloatBinary32Enabled();

        if (lstAppendMode.isEnabled()) {
            builder.withLocalSymbolTableAppendEnabled();
        } else {
            builder.withLocalSymbolTableAppendDisabled();
        }

        final IonWriter writer = builder.newWriter(out);

        final SymbolTable locals = writer.getSymbolTable();
        assertEquals(14, locals.getImportedMaxId());

        return writer;
    }
}
