/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.apps;

import com.amazon.ion.IonBinaryWriter;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.SymbolTable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;


public class EncodeApp
    extends BaseApp
{
    private SymbolTable[] myImports;
    private File myOutputDir;
    private String myOutputFile;


    //=========================================================================
    // Static methods

    public static void main(String[] args)
    {
        EncodeApp app = new EncodeApp();
        app.doMain(args);
    }


    //=========================================================================
    // Construction and Configuration

    public EncodeApp()
    {
    }


    //=========================================================================


    /**
     *
     * @param args
     * @return the next index to process
     */
    @Override
    protected int processOptions(String[] args)
    {
        ArrayList<SymbolTable> imports = new ArrayList<SymbolTable>();

        int i;
        for (i = 0; i < args.length; i++)
        {
            String arg = args[i];
            if ("--catalog".equals(arg))
            {
                String symtabPath = args[++i];
                loadCatalog(symtabPath);
            }
            else if ("--import".equals(arg))
            {
                // We'll use the latest version available.
                String name = args[++i];
                SymbolTable symtab = getLatestSharedSymtab(name);
                imports.add(symtab);
            }
            else if ("--output-dir".equals(arg))
            {
                String path = args[++i];
                myOutputDir = new File(path);
                if (! myOutputDir.isDirectory() || ! myOutputDir.canWrite())
                {
                    throw new RuntimeException("Not a writeable directory: "
                                               + path);
                }
            }
            else if ("--output".equals(arg))
	    {
		String path = args[++i];
                myOutputFile = path;
                myOutputDir = new File(path).getParentFile();
                if (! myOutputDir.isDirectory() || ! myOutputDir.canWrite())
                {
                    throw new RuntimeException("Not a writeable directory: "
                                               + path);
                }
            }
            else
            {
                // this arg is not an option, we're done here
                break;
            }
        }

        myImports = imports.toArray(new SymbolTable[0]);

        return i;
    }


    @Override
    protected void process(File inputFile, IonReader reader)
        throws IOException, IonException
    {
        IonBinaryWriter writer = mySystem.newBinaryWriter(myImports);

        writer.writeValues(reader);

        byte[] binaryBytes = writer.getBytes();

        if (myOutputDir != null)
        {
            String fileName = inputFile.getName();
            File outputFile = new File(myOutputDir, fileName);
            FileOutputStream out = new FileOutputStream(outputFile);
            try
            {
                out.write(binaryBytes);
            }
            finally
            {
                out.close();
            }
        }
        else
        {
            System.out.write(binaryBytes);
        }
    }

    @Override
    protected void process(IonReader reader)
        throws IOException, IonException
    {
        IonBinaryWriter writer = mySystem.newBinaryWriter(myImports);

        writer.writeValues(reader);

        byte[] binaryBytes = writer.getBytes();

        if (myOutputDir != null)
        {
            File outputFile = new File(myOutputFile);
            FileOutputStream out = new FileOutputStream(outputFile);
            try
            {
                out.write(binaryBytes);
            }
            finally
            {
                out.close();
            }
        }
        else
        {
            System.out.write(binaryBytes);
        }
    }
}
