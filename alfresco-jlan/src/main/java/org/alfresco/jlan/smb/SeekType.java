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

package org.alfresco.jlan.smb;

/**
 * Seek file position types.
 *
 * <p>Defines constants used by the SeekFile SMB request to specify where the seek position is relative to.
 *
 * @author gkspencer
 */
public enum SeekType {
    // Seek file types
    StartOfFile (0), CurrentPos (1), EndOfFile (2);
    int value;
    private SeekType(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    public static SeekType forValue(int value) {
        for (SeekType t : SeekType.values()) {
            if (t.value == value) {
                return t;
            }
        }
        return null;
    }
}
