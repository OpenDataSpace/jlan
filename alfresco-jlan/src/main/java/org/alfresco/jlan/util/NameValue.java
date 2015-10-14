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

package org.alfresco.jlan.util;

/**
 * Name Value Pair Class
 *
 * @author gkspencer
 */
public class NameValue {

    // Item name

    private final String m_name;

    // Item value

    private final Object m_value;

    /**
     * Class constructor
     *
     * @param name
     *            String
     * @param val
     *            Object
     */
    public NameValue(final String name, final Object val) {
        m_name = name;
        m_value = val;
    }

    /**
     * Return the item name
     *
     * @return String
     */
    public final String getName() {
        return m_name;
    }

    /**
     * Return the item value
     *
     * @return String
     */
    public final String getValue() {
        if (m_value instanceof String) {
            return (String) m_value;
        }
        return m_value.toString();
    }

    /**
     * Return the object value
     *
     * @return Object
     */
    public final Object getObject() {
        return m_value;
    }

    /**
     * Check if the value is a valid integer within the specified range
     *
     * @param low
     *            int
     * @param high
     *            int
     * @return int
     * @exception NumberFormatException
     */
    public final int getInteger(final int low, final int high) throws NumberFormatException {

        // Check if the value is valid

        if (m_value == null) {
            throw new NumberFormatException("No value");
        }

        // Convert the value to an integer

        final int ival = Integer.parseInt(getValue());

        // Check if the value is within the valid range

        if (ival < low || ival > high) {
            throw new NumberFormatException("Out of valid range");
        }

        // Return the integer value

        return ival;
    }

    /**
     * Check if the value is a valid long within the specified range
     *
     * @param low
     *            long
     * @param high
     *            long
     * @return long
     * @exception NumberFormatException
     */
    public final long getLong(final long low, final long high) throws NumberFormatException {

        // Check if the value is valid

        if (m_value == null) {
            throw new NumberFormatException("No value");
        }

        // Convert the value to a long

        final long lval = Long.parseLong(getValue());

        // Check if the value is within the valid range

        if (lval < low || lval > high) {
            throw new NumberFormatException("Out of valid range");
        }

        // Return the long value

        return lval;
    }

    /**
     * Return the name/value pair as a string
     *
     * @return String
     */
    @Override
    public String toString() {
        final StringBuffer str = new StringBuffer();

        str.append("[");
        str.append(getName());
        str.append(",");
        str.append(getValue());
        str.append("]");

        return str.toString();
    }
}
