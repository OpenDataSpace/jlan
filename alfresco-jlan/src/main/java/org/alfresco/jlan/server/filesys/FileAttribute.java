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

import org.alfresco.jlan.util.StringList;

/**
 *  SMB file attribute class.
 *
 *  <p>Defines various bit masks that may be returned in an FileInfo object, that
 *  is returned by the DiskInterface.getFileInformation () and SearchContext.nextFileInfo()
 *  methods.
 *
 * <p>The values are also used by the DiskInterface.StartSearch () method to determine
 *  the file/directory types that are returned.
 *
 * @see DiskInterface
 * @see SearchContext
 *
 * @author gkspencer
 */
public final class FileAttribute {
	/**
	 * Determine if the specified file attribute mask has the specified file attribute
	 * enabled.
	 *
	 * @return boolean
	 * @param attr int
	 * @param reqattr int
	 */
    public final static boolean hasAttribute(int attr, int reqattr) {
        // Check for the specified attribute
        if ((attr & reqattr) != 0)
            return true;
        return false;
    }

    /**
     * Check if the read-only attribute is set
     *
     * @param attr
     *            int
     * @return boolean
     */
    public static final boolean isReadOnly(int attr) {
        return (attr & FileAttributeType.ReadOnly.getFlag()) != 0 ? true : false;
    }

    /**
     * Check if the directory attribute is set
     *
     * @param attr
     *            int
     * @return boolean
     */
    public static final boolean isDirectory(int attr) {
        return (attr & FileAttributeType.Directory.getFlag()) != 0 ? true : false;
    }

    /**
     * Check if the hidden attribute is set
     *
     * @param attr
     *            int
     * @return boolean
     */
    public static final boolean isHidden(int attr) {
        return (attr & FileAttributeType.Hidden.getFlag()) != 0 ? true : false;
    }

    /**
     * Check if the system attribute is set
     *
     * @param attr
     *            int
     * @return boolean
     */
    public static final boolean isSystem(int attr) {
        return (attr & FileAttributeType.System.getFlag()) != 0 ? true : false;
    }

    /**
     * Check if the archive attribute is set
     *
     * @param attr
     *            int
     * @return boolean
     */
    public static final boolean isArchived(int attr) {
        return (attr & FileAttributeType.Archive.getFlag()) != 0 ? true : false;
    }

    /**
     * Return the specified file attributes as a comma seperated string
     *
     * @param attr
     *            int
     * @return String
     */
    public final static String getAttributesAsString(int attr) {
        // Check if no bits are set
        if (attr == 0)
            return "Normal";

        // Get a list of the attribute names, for attributes that are set
        StringList names = getAttributesAsList(attr);

        // Build the attribute names string
        StringBuffer str = new StringBuffer(128);
        for (int i = 0; i < names.numberOfStrings(); i++) {
            str.append(names.getStringAt(i));
            str.append(",");
        }

        // Trim the last comma
        if (str.length() > 0)
            str.setLength(str.length() - 1);

        // Return the attribute string
        return str.toString();
    }

    /**
     * Return the specified file attribute as a list of attribute names
     *
     * @param attr
     *            int
     * @return StringList
     */
    public final static StringList getAttributesAsList(final int attr) {
        final StringList names = new StringList();
        for (final FileAttributeType type : FileAttributeType.fromInt(attr)) {
            names.addString(type.toString());
        }
        return names;
    }

    /**
     * Return the specified NT file attributes as a comma separated string
     *
     * @param attr
     *            int
     * @return String
     */
    public final static String getNTAttributesAsString(final int attr) {
        // Get a list of the attribute names, for attributes that are set
        final StringList names = getNTAttributesAsList(attr);
        // Build the attribute names string
        StringBuffer str = new StringBuffer(128);
        for (int i = 0; i < names.numberOfStrings(); i++) {
            str.append(names.getStringAt(i));
            str.append(",");
        }
        // Trim the last comma
        if (str.length() > 0)
            str.setLength(str.length() - 1);

        // Return the attribute string
        return str.toString();
    }

    /**
     * Return the specified NT file attribute as a list of attribute names
     *
     * @param attr
     *            int
     * @return StringList
     */
    public final static StringList getNTAttributesAsList(final int attr) {
        final StringList names = new StringList();
        for (final NTFileAttributeType type : NTFileAttributeType.fromInt(attr)) {
            names.addString(type.toString());
        }
        return names;
    }
}
