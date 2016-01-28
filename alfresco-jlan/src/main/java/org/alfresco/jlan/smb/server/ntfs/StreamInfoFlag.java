package org.alfresco.jlan.smb.server.ntfs;

public enum StreamInfoFlag {
    SetStreamSize(0x0001), SetAllocationSize(0x0002), SetModifyDate(0x0004), SetCreationDate(0x0008), SetAccessDate(0x0010);
    private StreamInfoFlag(final int bit) {
    }
}
