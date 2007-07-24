/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.system;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.StaticSymbolTable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * A basic implementation of {@link IonCatalog} as a hash table.  There is no
 * automatic removal of entries.
 */
public class SimpleCatalog
    implements IonCatalog
{
    /*  CAVEATS AND LIMITATIONS
     *
     *  - When getTable can't find an exact match, it does a linear scan of
     *    all tables with the same name to find the greatest version.
     *  - Synchonization could probably be tighter using read/write locks
     *    instead of simple monitors.
     */
    private Map<String,TreeMap<Integer,StaticSymbolTable>> myTablesByName =
        new HashMap<String,TreeMap<Integer,StaticSymbolTable>>();


    public StaticSymbolTable getTable(String name)
    {
        TreeMap<Integer,StaticSymbolTable> versions;
        synchronized (myTablesByName)
        {
            versions = myTablesByName.get(name);
        }

        if (versions != null)
        {
            synchronized (versions)
            {
                Integer highestVersion = versions.lastKey();
                return versions.get(highestVersion);
            }
        }

        return null;
    }

    public StaticSymbolTable getTable(String name, int version)
    {
        TreeMap<Integer,StaticSymbolTable> versions;
        synchronized (myTablesByName)
        {
            versions = myTablesByName.get(name);
        }

        if (versions != null)
        {
            synchronized (versions)
            {
                StaticSymbolTable st = versions.get(version);
                if (st == null)
                {
                    // Scan the list for the greatest version.
                    assert !versions.isEmpty();
                    int maxVersion = 0;
                    for (StaticSymbolTable candidate : versions.values())
                    {
                        int candidateVersion = candidate.getVersion();
                        if (maxVersion < candidateVersion)
                        {
                            maxVersion = candidateVersion;
                            st = candidate;
                        }
                    }
                    assert st != null;
                }

                return st;
            }
        }

        return null;
    }

    public void putTable(StaticSymbolTable table)
    {
        String name = table.getName();
        int version = table.getVersion();
        assert version >= 0;

        synchronized (myTablesByName)
        {
            TreeMap<Integer,StaticSymbolTable> versions =
                myTablesByName.get(name);
            if (versions == null)
            {
                versions = new TreeMap<Integer,StaticSymbolTable>();
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
    public StaticSymbolTable removeTable(String name, int version)
    {
        StaticSymbolTable removed = null;

        synchronized (myTablesByName)
        {
            TreeMap<Integer,StaticSymbolTable> versions =
                myTablesByName.get(name);
            if (versions != null)
            {
                removed = versions.remove(version);

                // Remove empty helper tables
                if (versions.isEmpty())
                {
                    myTablesByName.remove(name);
                }
            }
        }

        return removed;
    }
}
