package org.alfresco.jlan.server.filesys;

public enum GrantedFileAccess {
    UNKNOWN (-1, "Unknown"), ATTRIBUTESONLY(0, "AttributesOnly"), READONLY(1, "ReadOnly"), WRITEONLY(2, "WriteOnly"), READWRITE(3, "ReadWrite");
    private int value;
    private String name;

    private GrantedFileAccess(final int value, final String name) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return name;
    }
}
