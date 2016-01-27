package org.alfresco.jlan.server.filesys;

public enum FileStatusFlag {
    IOPending(0x0001), DeleteOnClose(0x0002), DelayedWriteError(0x0004), Created(0x0008), DelayedClose(0x0010);
    private int flag;

    private FileStatusFlag(final int bitFlag) {
        this.flag = bitFlag;
    }
    
    public int getFlag() {
        return flag;
    }
}
