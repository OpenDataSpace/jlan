package org.alfresco.jlan.server.filesys;

public enum AllowedFileAccess {
    ATTRIBUTESONLY(0), READONLY (1), WRITEONLY (2), READWRITE (3);
    private int value;
    private AllowedFileAccess(final int value) {
        this.value = value;
    }
}
