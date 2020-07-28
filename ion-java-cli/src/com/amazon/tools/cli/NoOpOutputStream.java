package com.amazon.tools.cli;

import java.io.OutputStream;

public class NoOpOutputStream extends OutputStream {
    @Override
    public void write(int b) {
        return;
    }
}
