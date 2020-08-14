package tools.events;

import com.amazon.ion.SymbolTable;

public class ImportDescriptor {
    private final String importName;
    private final int maxId;
    private final int version;

    public ImportDescriptor(String importName, int maxId, int version) {
        this.importName = importName;
        this.maxId = maxId;
        this.version = version;
    }

    public ImportDescriptor(SymbolTable symbolTable) {
        this.importName = symbolTable.getName();
        this.maxId = symbolTable.getMaxId();
        this.version = symbolTable.getVersion();
    }

    public int getVersion() {
        return version;
    }

    public int getMaxId() {
        return maxId;
    }

    public String getImportName() {
        return importName;
    }
}
