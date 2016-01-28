package org.alfresco.jlan.smb;

import java.util.EnumSet;

public enum ServerTypeFlag {
    WorkStation         (0x00000001, "Workstation"),
    Server              (0x00000002, "Server"),
    SQLServer           (0x00000004, "SQLServer"),
    DomainCtrl          (0x00000008, "DomainController"),
    DomainBakCtrl       (0x00000010, "BackupDomainController"),
    TimeSource          (0x00000020, "TimeSource"),
    AFPServer           (0x00000040, "AFPServer"),
    NovellServer        (0x00000080, "NovellServer"),
    DomainMember        (0x00000100, "DomainMember"),
    PrintServer         (0x00000200, "PrintServer"),
    DialinServer        (0x00000400, "DialinServer"),
    UnixServer          (0x00000800, "UnixServer"),
    NTServer            (0x00001000, "NTServer"),
    WfwServer           (0x00002000, "WfwServer"),
    MFPNServer          (0x00004000, "MFPNServer"),
    NTNonDCServer       (0x00008000, "NtNonDCServer"),
    PotentialBrowse     (0x00010000, "PotentialBrowse"),
    BackupBrowser       (0x00020000, "BackupBrowser"),
    MasterBrowser       (0x00040000, "MasterBrowser"),
    DomainMaster        (0x00080000, "DomainMaster"),
    OSFServer           (0x00100000, "OSFServer"),
    VMSServer           (0x00200000, "VMSServer"),
    Win95Plus           (0x00400000, "Win95Plus"),
    DFSRoot             (0x00800000, "DFSRoot"),
    NTCluster           (0x01000000, "NTCluster"),
    TerminalServer      (0x02000000, "TerminalServer"),
    DCEServer           (0x10000000, ""),
    AlternateXport      (0x20000000, ""),
    LocalListOnly       (0x40000000, "DCEServer"),
    DomainEnum          (0x80000000, "");

    private final String name;
    private final int bit;
    private ServerTypeFlag(final int bit, final String name) {
        this.bit = bit;
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    public static EnumSet<ServerTypeFlag> fromInt(final int flags) {
        final EnumSet<ServerTypeFlag> result = EnumSet.noneOf(ServerTypeFlag.class);
        for (ServerTypeFlag flag : ServerTypeFlag.values()) {
            if ((flags & flag.bit) != 0) {
                result.add(flag);
            }
        }
        return result;
    }

    public static int toInt(EnumSet<ServerTypeFlag> flags) {
        int result = 0;
        for (ServerTypeFlag flag : flags) {
            result += flag.bit;
        }
        return result;
    }
}
