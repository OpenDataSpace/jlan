package org.alfresco.jlan.server.filesys;

import java.util.EnumSet;

public class FileStatusFlags {

    private EnumSet<FileStatusFlag> flags;

    public FileStatusFlags(final EnumSet<FileStatusFlag> flags) {
        this.flags = EnumSet.copyOf(flags);
    }

    public FileStatusFlags() {
        this(EnumSet.noneOf(FileStatusFlag.class));
    }

    public EnumSet<FileStatusFlag> getFlags() {
        return this.flags.clone();
    }

    public void setFlags(EnumSet<FileStatusFlag> flags) {
        this.flags.clear();
        this.flags.addAll(flags);
    }

    public int getValue() {
        int result = 0;
        for (FileStatusFlag flag : flags) {
            result += flag.getFlag();
        }
        return result;
    }
}
