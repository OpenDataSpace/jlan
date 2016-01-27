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
 * File Status Class
 *
 * @author gkspencer
 */
public enum FileStatus {
    Unknown(-1, "Unknown"), NotExist(0, "NotExist"), FileExists(1, "FileExists"), DirectoryExists(2, "DirExists");

    private int value;
    private String name;

    private FileStatus(final int value, final String name) {
        this.value = value;
        this.name = name;
    }

    public static FileStatus forValue(final int value) {
        for (FileStatus t : FileStatus.values()) {
            if (t.value == value) {
                return t;
            }
        }
        return FileStatus.Unknown;
    }

    @Override
    public String toString() {
        return name;
    }
}
