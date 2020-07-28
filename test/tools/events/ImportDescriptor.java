package tools.events;

import com.amazon.ion.SymbolTable;

public class ImportDescriptor {
    private String import_name;
    private int max_id;
    private int version;

    public ImportDescriptor(String import_name, int max_id, int version){
        this.import_name = import_name;
        this.max_id = max_id;
        this.version = version;
    }

    public ImportDescriptor(SymbolTable symbolTable){
        this.import_name = symbolTable.getName();
        this.max_id = symbolTable.getMaxId();
        this.version = symbolTable.getVersion();
    }

    public void setImport_name(String import_name) {
        this.import_name = import_name;
    }

    public void setMax_id(int max_id) {
        this.max_id = max_id;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    public int getMax_id() {
        return max_id;
    }

    public String getImport_name() {
        return import_name;
    }
}
