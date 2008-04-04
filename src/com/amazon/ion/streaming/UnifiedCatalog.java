package com.amazon.ion.streaming;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.StaticSymbolTable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


/**
 * A basic implementation of {@link IonCatalog} as a hash table.  Based on 
 * SimpleCatalog but operating over UnifiedSymbolTables rather that the
 * base StaticSymbolTables.  Like SimpleCatalog there is no automatic removal 
 * of entries.
 */

public class UnifiedCatalog
	implements IonCatalog
{
    /*  CAVEATS AND LIMITATIONS
     *
     *  - When getTable can't find an exact match, it does a linear scan of
     *    all tables with the same name to find the greatest version.
     *  - Synchonization could probably be tighter using read/write locks
     *    instead of simple monitors.
     */
    private Map<String,TreeMap<Integer,UnifiedSymbolTable>> myTablesByName =
        new HashMap<String,TreeMap<Integer,UnifiedSymbolTable>>();

    public UnifiedSymbolTable getTable(String name)
    {
        TreeMap<Integer,UnifiedSymbolTable> versions;
        synchronized (myTablesByName)
        {
            versions = myTablesByName.get(name);
        }

        if (versions == null) return null;
        
        synchronized (versions)
        {
            Integer highestVersion = versions.lastKey();
            return versions.get(highestVersion);
        }
    }

    public UnifiedSymbolTable getTable(String name, int version)
    {
        TreeMap<Integer,UnifiedSymbolTable> versions;
        UnifiedSymbolTable st = null;
        
        synchronized (myTablesByName)
        {
            versions = myTablesByName.get(name);
        }
        if (versions == null) return null;
        
        synchronized (versions)
        {
        	st = versions.get(version);
            if (st == null) {
            	// if we don't have the one you want, we'll give you the
            	// "best" one we have, even if it's newer than what you
            	// asked for (see CAVEAT above)
                assert !versions.isEmpty();
                
                // FIXME in Java 5 this works:                
                st = versions.get(versions.lastKey());
                
                // FIXME in Java 6 this works:
                //Map.Entry<Integer, UnifiedSymbolTable> entry;
                //entry = versions.lastEntry();
                //assert entry != null;
                //st = entry.getValue();
                
                assert st != null;
            }
        }
        return st;
    }

	public void putTable(StaticSymbolTable table) 
	{
		UnifiedSymbolTable utable;
		if (!(table instanceof UnifiedSymbolTable)) {
			// the hard way
			utable = new UnifiedSymbolTable(table);
		}
		else {
			utable = (UnifiedSymbolTable)table;
		}
		putTable(utable);
	}

    public void putTable(UnifiedSymbolTable table)
    {
        String name = table.getName();
        int version = table.getVersion();
        assert version >= 0;

        synchronized (myTablesByName)
        {
            TreeMap<Integer,UnifiedSymbolTable> versions;
            
            versions = myTablesByName.get(name);
            if (versions == null)
            {
                versions = new TreeMap<Integer,UnifiedSymbolTable>();
                myTablesByName.put(name, versions);
            }
            versions.put(version, table);
        }
    }

    /**
     * Removes a symbol table from this catalog.
     *
     * @return the removed table, or <code>null</code> if this catalog has
     * no matching table.
     */
    public UnifiedSymbolTable removeTable(String name, int version)
    {
    	UnifiedSymbolTable removed = null;

        synchronized (myTablesByName)
        {
            TreeMap<Integer,UnifiedSymbolTable> versions;
            
            versions = myTablesByName.get(name);
            if (versions != null) {
                removed = versions.remove(version);

                // Remove empty helper tables
                if (versions.isEmpty()) {
                    myTablesByName.remove(name);
                }
            }
        }
        return removed;
    }
}
