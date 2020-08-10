package tools.events;

import com.amazon.ion.SymbolTable;

public class ImportDescriptor {
    private String importName;
    private int maxId;
    private int version;

    public ImportDescriptor() {
        this.importName = null;
        this.maxId = -1;
        this.version = -1;
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

    public void setImportName(String importName) {
        this.importName = importName;
    }

    public void setMaxId(int maxId) {
        this.maxId = maxId;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
