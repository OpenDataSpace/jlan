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

import java.util.EnumSet;

/**
 * Device Attribute Constants Class
 *
 * <p>
 * Specifies the constants that can be used to set the DiskDeviceContext device attributes.
 *
 * @author gkspencer
 */
public enum DeviceAttribute {
    Removable(0x0001), ReadOnly(0x0002), FloppyDisk(0x0004), WriteOnce(0x0008), Remote(0x0010), Mounted(0x0020), Virtual(0x0040);
    private final int bit;
    private DeviceAttribute(final int flag) {
        this.bit = flag;
    }
    
    public static EnumSet<DeviceAttribute> fromInt(final int flag) {
        final EnumSet<DeviceAttribute> result = EnumSet.noneOf(DeviceAttribute.class);
        for (DeviceAttribute attribute : DeviceAttribute.values()) {
            if ((attribute.bit & flag) != 0) {
                result.add(attribute);
            }
        }
        return result;
    }
    
    public static int asInt(final EnumSet<DeviceAttribute> attributes) {
        int result = 0;
        for (DeviceAttribute attribute : attributes) {
            result += attribute.bit;
        }
        return result;
    }
}
