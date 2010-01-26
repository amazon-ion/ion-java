// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.apps;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.SystemFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * TODO pretty-print on/off and configure
 * TODO show system view (eg symbol tables)
 */
public class PrintApp
    extends BaseApp
{
    private File myOutputDir;
    private String myOutputFile;


    //=========================================================================
    // Static methods

    public static void main(String[] args)
    {
        PrintApp app = new PrintApp();
        app.doMain(args);
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
        for (int i = 0; i < args.length; i++)
        {
            String arg = args[i];
            if ("--catalog".equals(arg))
            {
                String symtabPath = args[++i];
                loadCatalog(symtabPath);
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
            // TODO --help
            else
            {
                // this arg is not an option, we're done here
                return i;
            }
        }

        return args.length;
    }


    @Override
    protected void process(File inputFile, IonReader reader)
        throws IOException, IonException
    {
        if (myOutputDir == null)
        {
            process(reader, System.out);
        }
        else
        {
            String fileName = inputFile.getName();
            File outputFile = new File(myOutputDir, fileName);
            FileOutputStream out = new FileOutputStream(outputFile);
            try
            {
                process(reader, out);
            }
            finally
            {
                out.close();
            }
        }
    }

    @Override
    protected void process(IonReader reader)
        throws IOException, IonException
    {
        if (myOutputDir == null)
        {
            process(reader, System.out);
        }
        else
        {
            File outputFile = new File(myOutputFile);
            FileOutputStream out = new FileOutputStream(outputFile);
            try
            {
                process(reader, out);
            }
            finally
            {
                out.close();
            }
        }
    }

    protected void process(IonReader reader, OutputStream out)
        throws IOException, IonException
    {
        IonSystem system = reader.getSystem();
        if (system == null) {
            system = SystemFactory.newSystem();
        }
        IonWriter writer = system.newTextWriter(out);  // was new IonTextWriter(out, true);

        writer.writeValues(reader);

        // Ensure there's a newline at the end and flush the buffer.
        out.write('\n');
        out.flush();
    }
}
