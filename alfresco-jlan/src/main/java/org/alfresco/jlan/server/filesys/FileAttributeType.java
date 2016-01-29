package org.alfresco.jlan.server.filesys;

import java.util.EnumSet;

public enum FileAttributeType {
    Normal(0x00, null), ReadOnly(0x01, "ReadOnly"), Hidden(0x02, "Hidden"), System(0x04, "System"), Volume(0x08, "Volume"), Directory(0x10,
            "Directory"), Archive(0x20, "Archive");
    private final int flag;
    private final String name;

    private FileAttributeType(final int flag, final String name) {
        this.flag = flag;
        this.name = name;
    }

    public int getFlag() {
        return flag;
    }

    @Override
    public String toString() {
        return name;
    }

    public static EnumSet<FileAttributeType> fromInt(final int attributes) {
        final EnumSet<FileAttributeType> result = EnumSet.noneOf(FileAttributeType.class);
        for (FileAttributeType type : FileAttributeType.values()) {
            if ((type.flag & attributes) != 0) {
                result.add(type);
            }
        }
        return result;
    }
}
