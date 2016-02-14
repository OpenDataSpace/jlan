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

package org.alfresco.jlan.smb.nt;

/**
 * Reparse Point Class
 *
 * <p>
 * Contains reparse point constants.
 *
 * @author gkspencer
 */
public enum ReparsePoint {
    Unknown(0, "Unknown"), TypeDFS(0x8000000A, "DFS"), TypeDFSR(0x80000012, "DFSR"), TypeHSM(0xC0000004, "HSM"), TypeHSM2(0x80000006,
            "HSM2"), TypeMountPoint(0xA0000003, "MountPoint"), TypeSIS(0x80000007, "SIS"), TypeSymLink(0xA000000C, "SymLink");

    private final int value;
    private final String name;

    private ReparsePoint(final int value, final String name) {
        this.value = value;
        this.name = name;
    }

    public int getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return name;
    }

    public static ReparsePoint fromInt(int reparseType) {
        for (ReparsePoint point : ReparsePoint.values()) {
            if (point.value == reparseType) {
                return point;
            }
        }
        return ReparsePoint.Unknown;
    }
}
