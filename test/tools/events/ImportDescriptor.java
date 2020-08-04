package tools.events;

import com.amazon.ion.SymbolTable;

public class ImportDescriptor {
    private final String import_name;
    private final int max_id;
    private final int version;

    public ImportDescriptor(SymbolTable symbolTable) {
        this.import_name = symbolTable.getName();
        this.max_id = symbolTable.getMaxId();
        this.version = symbolTable.getVersion();
    }

    final int getVersion() {
        return version;
    }

    final int getMax_id() {
        return max_id;
    }

    final String getImport_name() {
        return import_name;
    }
}
