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

package org.alfresco.jlan.server.auth.acl;

import java.util.Enumeration;
import java.util.List;

import org.alfresco.jlan.server.SrvSession;
import org.alfresco.jlan.server.config.InvalidConfigurationException;
import org.alfresco.jlan.server.config.ServerConfiguration;
import org.alfresco.jlan.server.core.SharedDevice;
import org.alfresco.jlan.server.core.SharedDeviceList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;
import org.springframework.extensions.config.ConfigElement;

/**
 * Default Access Control Manager Class
 *
 * <p>
 * Default access control manager implementation.
 *
 * @author gkspencer
 */
public class DefaultAccessControlManager implements AccessControlManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAccessControlManager.class);

    // Access control factory
    private final AccessControlFactory m_factory;

    // Debug enable flag
    private boolean m_debug;

    /**
     * Class constructor
     */
    public DefaultAccessControlManager() {
        // Create the access control factory
        m_factory = new AccessControlFactory();
    }

    /**
     * Check if the session has access to the shared device.
     *
     * @param sess
     *            SrvSession
     * @param share
     *            SharedDevice
     * @return int
     */
    @Override
    public int checkAccessControl(final SrvSession sess, final SharedDevice share) {
        // Check if the shared device has any access control configured
        if (share.hasAccessControls() == false) {
            if (LOGGER.isInfoEnabled() && hasDebug()) {
                LOGGER.info(MarkerFactory.getMarker(sess.getUniqueId()), "Check access control for " + share.getName() + ", no ACLs");
            }

            // Allow full access to the share
            return AccessControl.ReadWrite;
        }

        // Process the access control list
        final AccessControlList acls = share.getAccessControls();
        int access = AccessControl.Default;

        if (LOGGER.isInfoEnabled() && hasDebug()) {
            LOGGER.info(MarkerFactory.getMarker(sess.getUniqueId()), "Check access control for " + share.getName() + ", ACLs=" + acls.numberOfControls());
        }

        for (int i = 0; i < acls.numberOfControls(); i++) {
            // Get the current access control and run
            final AccessControl acl = acls.getControlAt(i);
            final int curAccess = acl.allowsAccess(sess, share, this);

            if (LOGGER.isInfoEnabled() && hasDebug()) {
                LOGGER.info(MarkerFactory.getMarker(sess.getUniqueId()), "  Check access ACL=" + acl + ", access=" + AccessControl.asAccessString(curAccess));
            }

            // Update the allowed access
            if (curAccess != AccessControl.Default) {
                access = curAccess;
            }
        }

        // Check if the default access level is still selected, if so then get the default level from the
        // access control list
        if (access == AccessControl.Default) {
            // Use the default access level
            access = acls.getDefaultAccessLevel();
            if (LOGGER.isInfoEnabled() && hasDebug()) {
                LOGGER.info(MarkerFactory.getMarker(sess.getUniqueId()), "Access defaulted=" + AccessControl.asAccessString(access) + ", share=" + share);
            }
        } else if (LOGGER.isInfoEnabled() && hasDebug()) {
            LOGGER.info(MarkerFactory.getMarker(sess.getUniqueId()), "Access allowed=" + AccessControl.asAccessString(access) + ", share=" + share);
        }

        // Return the access type
        return access;
    }

    /**
     * Filter the list of shared devices to return a list that contains only the shares that are visible or accessible by the session.
     *
     * @param sess
     *            SrvSession
     * @param shares
     *            SharedDeviceList
     * @return SharedDeviceList
     */
    @Override
    public SharedDeviceList filterShareList(final SrvSession sess, final SharedDeviceList shares) {
        // Check if the share list is valid or empty
        if (shares == null || shares.numberOfShares() == 0) {
            return shares;
        }

        if (LOGGER.isInfoEnabled() && hasDebug()) {
            LOGGER.info(MarkerFactory.getMarker(sess.getUniqueId()), "Filter share list for " + sess + ", shares=" + shares);
        }

        // For each share in the list check the access, remove any shares that the session does not
        // have access to.
        final SharedDeviceList filterList = new SharedDeviceList();
        final Enumeration<SharedDevice> enm = shares.enumerateShares();
        while (enm.hasMoreElements()) {
            // Get the current share
            final SharedDevice share = enm.nextElement();

            // Check if the share has any access controls
            if (share.hasAccessControls()) {
                // Check if the session has access to this share
                final int access = checkAccessControl(sess, share);
                if (access != AccessControl.NoAccess) {
                    filterList.addShare(share);
                }
            } else {
                // Add the share to the filtered list
                filterList.addShare(share);
            }
        }

        if (LOGGER.isInfoEnabled() && hasDebug()) {
            LOGGER.info(MarkerFactory.getMarker(sess.getUniqueId()), "Filtered share list " + filterList);
        }

        // Return the filtered share list
        return filterList;
    }

    /**
     * Initialize the access control manager
     *
     * @param config
     *            ServerConfiguration
     * @param params
     *            ConfigElement
     * @throws InvalidConfigurationException
     */
    @Override
    public void initialize(final ServerConfiguration config, final ConfigElement params) throws InvalidConfigurationException {
        // Check if debug output is enabled
        if (params.getChild("debug") != null) {
            setDebug(true);
        }

        // Add the default access control types
        addAccessControlType(new UserAccessControlParser());
        addAccessControlType(new ProtocolAccessControlParser());
        addAccessControlType(new DomainAccessControlParser());
        addAccessControlType(new IpAddressAccessControlParser());
        addAccessControlType(new GidAccessControlParser());
        addAccessControlType(new UidAccessControlParser());

        // Check if there are any custom access control rules
        final ConfigElement ruleList = params.getChild("rule");
        if (ruleList != null && ruleList.hasChildren()) {
            // Add the custom rule types
            final List<ConfigElement> rules = ruleList.getChildren();
            for (final ConfigElement ruleVal : rules) {
                if (ruleVal.getValue() == null || ruleVal.getValue().length() == 0) {
                    throw new InvalidConfigurationException("Empty rule definition");
                }

                // Create an instance of the rule parser and check that it is based on the access control
                // parser class.
                try {
                    // Create an instance of the rule parser class
                    final Object ruleObj = Class.forName(ruleVal.getValue()).newInstance();

                    // Check if the class is an access control parser
                    if (ruleObj instanceof AccessControlParser) {
                        // Add the new rule type
                        addAccessControlType((AccessControlParser) ruleObj);
                    }
                } catch (final ClassNotFoundException ex) {
                    throw new InvalidConfigurationException("Rule class not found, " + ruleVal.getValue());
                } catch (final InstantiationException ex) {
                    throw new InvalidConfigurationException("Error creating rule object, " + ruleVal.getValue() + ", " + ex.toString());
                } catch (final IllegalAccessException ex) {
                    throw new InvalidConfigurationException("Error creating rule object, " + ruleVal.getValue() + ", " + ex.toString());
                }
            }
        }
    }

    /**
     * Create an access control.
     *
     * @param type
     *            String
     * @param params
     *            ConfigElement
     * @return AccessControl
     * @throws ACLParseException
     * @throws InvalidACLTypeException
     */
    @Override
    public AccessControl createAccessControl(final String type, final ConfigElement params) throws ACLParseException, InvalidACLTypeException {
        // Use the access control factory to create the access control instance
        return m_factory.createAccessControl(type, params);
    }

    /**
     * Add an access control parser to the list of available access control types.
     *
     * @param parser
     *            AccessControlParser
     */
    @Override
    public void addAccessControlType(final AccessControlParser parser) {
        if (LOGGER.isInfoEnabled() && hasDebug()) {
            LOGGER.info("AccessControlManager Add rule type " + parser.getType());
        }

        // Add the new access control type to the factory
        m_factory.addParser(parser);
    }

    /**
     * Determine if debug output is enabled
     *
     * @return boolean
     */
    public final boolean hasDebug() {
        return m_debug;
    }

    /**
     * Enable/disable debug output
     *
     * @param dbg
     *            boolean
     */
    public final void setDebug(final boolean dbg) {
        m_debug = dbg;
    }
}
