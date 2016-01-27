/*
 * Copyright (C) 2006-2010 Alfresco Software Limited.
 *
 * This file is part of Alfresco
 *
 * Alfresco is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Alfresco is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Alfresco. If not, see <http://www.gnu.org/licenses/>.
 */

package org.alfresco.jlan.server.filesys;

/**
 * File Type Enumeration
 *
 * <p>
 * File type constants.
 *
 * @author gkspencer
 */
public enum FileType {
    UNKNOWN(0, "Unknown"),
    REGULAR_FILE(1, "File"),
    DIRECTORY(2, "Directory"),
    SYMBOLIC_LINK(3, "SymbolicLink"),
    HARD_LINK(4, "HardLink"),
    DEVICE(5, "Device");

    private int value;
    private String name;

    private FileType(final int value, final String name) {
        this.value = value;
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    public FileType forValue(final int value) {
        for (FileType t : FileType.values()) {
            if (t.value == value) {
                return t;
            }
        }
        return UNKNOWN;
    }
}
