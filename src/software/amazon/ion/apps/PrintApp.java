/*
 * Copyright 2008-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.apps;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonWriter;

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
        IonSystem system = this.mySystem;
        IonWriter writer = system.newTextWriter(out);

        writer.writeValues(reader);

        // Ensure there's a newline at the end and flush the buffer.
        out.write('\n');
        out.flush();
    }
}
