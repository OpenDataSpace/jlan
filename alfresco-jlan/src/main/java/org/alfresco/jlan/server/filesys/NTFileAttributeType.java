package org.alfresco.jlan.server.filesys;

import java.util.EnumSet;

public enum NTFileAttributeType {
    ReadOnly(0x00000001, "ReadOnly"), Hidden(0x00000002, "Hidden"), System(0x00000004, "System"), VolumeId(0x00000008, "VolumeId"), Directory(0x00000010,
            "Directory"), Archive(0x00000020, "Archive"), Device(0x00000040, "Device"), Normal(0x00000080, "Normal"), Temporary(0x00000100,
                    "Temporary"), Sparse(0x00000200, "Sparse"), ReparsePoint(0x00000400, "ReparsePoint"), Compressed(0x00000800, "Compressed"), Offline(
                            0x00001000, "Offline"), Indexed(0x00002000, "Indexed"), Encrypted(0x00004000, "Encrypted"), OpenNoRecall(0x00100000,
                                    "OpenNoRecall"), OpenReparsePoint(0x00200000, "OpenReparsePoint"), PosixSemantics(0x01000000,
                                            "PosixSemantics"), BackupSemantics(0x02000000, ""), DeleteOnClose(0x04000000, "DeleteOnClose"), SequentialScan(
                                                    0x08000000, "SequentialScan"), RandomAccess(0x10000000, "RandomAccess"), NoBuffering(0x20000000,
                                                            "NoBuffering"), Overlapped(0x40000000, "Overlapped"), WriteThrough(0x80000000, "WriteThrough");

    private final int flag;
    private final String name;

    private NTFileAttributeType(final int flag, final String name) {
        this.flag = flag;
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    public int getFlag() {
        return flag;
    }

    public static EnumSet<NTFileAttributeType> fromInt(int attributes) {
        final EnumSet<NTFileAttributeType> result = EnumSet.noneOf(NTFileAttributeType.class);
        for (NTFileAttributeType type : NTFileAttributeType.values()) {
            if ((type.flag & attributes) != 0) {
                result.add(type);
            }
        }
        return result;
    }
}
