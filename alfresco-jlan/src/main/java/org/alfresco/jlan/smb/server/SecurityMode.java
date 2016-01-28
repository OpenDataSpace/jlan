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

package org.alfresco.jlan.smb.server;

import java.util.EnumSet;

/**
 * Security Mode Class
 *
 * <p>
 * CIFS security mode constants.
 *
 * @author gkspencer
 */
public enum SecurityMode {
    // Security mode flags returned in the SMB negotiate response
    UserMode(0x0001), EncryptedPasswords(0x0002), SignaturesEnabled(0x0004), SignaturesRequired(0x0008);
    private final int bit;

    private SecurityMode(final int bit) {
        this.bit = bit;
    }
    
    public static int asInt(EnumSet<SecurityMode> modes) {
        int result = 0;
        for (SecurityMode mode : modes) {
            result += mode.bit;
        }
        return result;
    }
}
