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

import java.util.EnumSet;

import org.alfresco.jlan.util.StringList;

/**
 * Server Type Flags Class
 *
 * <p>Defines server type flags that may be returned in a ServerInfo object.
 *
 * @author gkspencer
 */
public class ServerType {

	/**
	 * Convert server type flags to a list of server type strings
	 *
	 * @param typ int
	 * @return Vector
	 */
	public static final StringList TypeAsStrings(final EnumSet<ServerTypeFlag> flags) {
		StringList strs = new StringList();
		for (ServerTypeFlag flag : flags) {
		    strs.addString(flag.toString());
		}
		return strs;
	}
}
