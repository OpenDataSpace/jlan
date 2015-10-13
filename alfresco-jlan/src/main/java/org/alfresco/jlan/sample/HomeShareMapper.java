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

package org.alfresco.jlan.sample;

import java.util.Enumeration;

import org.alfresco.jlan.server.SrvSession;
import org.alfresco.jlan.server.auth.ClientInfo;
import org.alfresco.jlan.server.auth.InvalidUserException;
import org.alfresco.jlan.server.auth.UserAccount;
import org.alfresco.jlan.server.config.ConfigId;
import org.alfresco.jlan.server.config.ConfigurationListener;
import org.alfresco.jlan.server.config.InvalidConfigurationException;
import org.alfresco.jlan.server.config.SecurityConfigSection;
import org.alfresco.jlan.server.config.ServerConfiguration;
import org.alfresco.jlan.server.core.ShareMapper;
import org.alfresco.jlan.server.core.ShareType;
import org.alfresco.jlan.server.core.SharedDevice;
import org.alfresco.jlan.server.core.SharedDeviceList;
import org.alfresco.jlan.server.filesys.DiskDeviceContext;
import org.alfresco.jlan.server.filesys.DiskInterface;
import org.alfresco.jlan.server.filesys.DiskSharedDevice;
import org.alfresco.jlan.server.filesys.FilesystemsConfigSection;
import org.alfresco.jlan.smb.server.disk.JavaFileDiskDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.extensions.config.ConfigElement;

/**
 * Default Share Mapper Class
 *
 * <p>
 * Maps disk and print share lookup requests to the list of shares defined in the server configuration.
 *
 * @author gkspencer
 */
public class HomeShareMapper implements ShareMapper, ConfigurationListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(HomeShareMapper.class);

    // Home directory share name
    public static final String HOME_DIRECTORY_SHARE = "HOME";

    // Server configuration and configuration sections
    private ServerConfiguration m_config;
    private SecurityConfigSection m_securityConfig;
    private FilesystemsConfigSection m_filesysConfig;

    // Debug enable flag
    private boolean m_debug;

    /**
     * Default constructor
     */
    public HomeShareMapper() {
    }

    /**
     * Initialize the share mapper
     *
     * @param config
     *            ServerConfiguration
     * @param params
     *            ConfigElement
     * @exception InvalidConfigurationException
     */
    @Override
    public void initializeMapper(final ServerConfiguration config, final ConfigElement params) throws InvalidConfigurationException {
        // Save the server configuration
        m_config = config;

        // Get the required configuration sections
        m_securityConfig = (SecurityConfigSection) m_config.getConfigSection(SecurityConfigSection.SectionName);

        // Filesystem configuration will usually be initialized after the security configuration so we need to plug in
        // a listener to initialize it later
        m_filesysConfig = (FilesystemsConfigSection) m_config.getConfigSection(FilesystemsConfigSection.SectionName);
        if (m_filesysConfig == null) {
            m_config.addListener(this);
        }
        // Check if debug is enabled
        if (params.getChild("debug") != null) {
            m_debug = true;
        }
    }

    /**
     * Check if debug output is enabled
     *
     * @return boolean
     */
    public final boolean hasDebug() {
        return m_debug;
    }

    /**
     * Find a share using the name and type for the specified client.
     *
     * @param host
     *            String
     * @param name
     *            String
     * @param typ
     *            int
     * @param sess
     *            SrvSession
     * @param create
     *            boolean
     * @return SharedDevice
     * @exception InvalidUserException
     */
    @Override
    public SharedDevice findShare(final String host, final String name, final int typ, final SrvSession sess, final boolean create)
            throws InvalidUserException {
        // Check for the special HOME disk share
        SharedDevice share = null;
        if ((typ == ShareType.DISK || typ == ShareType.UNKNOWN) && name.compareTo(HOME_DIRECTORY_SHARE) == 0 && sess.getClientInformation() != null) {
            // Get the client details
            final ClientInfo client = sess.getClientInformation();
            if (LOGGER.isInfoEnabled() && hasDebug()) {
                LOGGER.info("Map share " + name + ", type=" + ShareType.TypeAsString(typ) + ", client=" + client);
            }

            // Find the user account details and check if a home directory has been configured
            final UserAccount acc = m_securityConfig.getUserAccounts().findUser(client.getUserName());
            if (acc != null && acc.hasHomeDirectory()) {
                // Check if the share has already been created for the session
                if (sess.hasDynamicShares()) {
                    // Check if the required share exists in the sessions dynamic share list
                    share = sess.getDynamicShareList().findShare(name, typ, false);
                    if (LOGGER.isInfoEnabled() && hasDebug() && share != null) {
                        LOGGER.info("  Reusing existing dynamic share for " + name);
                    }
                }

                // Check if we found a share, if not then create a new dynamic share for the home directory
                if (share == null && create == true) {
                    // Create the disk driver and context
                    final DiskInterface diskDrv = new JavaFileDiskDriver();
                    final DiskDeviceContext diskCtx = new DiskDeviceContext(acc.getHomeDirectory());

                    // Create a temporary shared device for the users home directory
                    final DiskSharedDevice diskShare = new DiskSharedDevice(HOME_DIRECTORY_SHARE, diskDrv, diskCtx, SharedDevice.Temporary);

                    // Add the new share to the sessions dynamic share list
                    sess.addDynamicShare(diskShare);
                    share = diskShare;

                    if (LOGGER.isInfoEnabled() && hasDebug()) {
                        LOGGER.info("  Mapped share " + name + " to " + acc.getHomeDirectory());
                    }
                }
            } else {
                throw new InvalidUserException("No home directory");
            }
        } else {
            // Find the required share by name/type. Use a case sensitive search first, if that fails use a case
            // insensitive search.
            share = m_filesysConfig.getShares().findShare(name, typ, false);
            if (share == null) {
                // Try a case insensitive search for the required share
                share = m_filesysConfig.getShares().findShare(name, typ, true);
            }
        }

        // Check if the share is available
        if (share != null && share.getContext() != null && share.getContext().isAvailable() == false) {
            share = null;
        }

        // Return the shared device, or null if no matching device was found
        return share;
    }

    /**
     * Delete temporary shares for the specified session
     *
     * @param sess
     *            SrvSession
     */
    @Override
    public void deleteShares(final SrvSession sess) {
        // Check if the session has any dynamic shares
        if (sess.hasDynamicShares() == false) {
            return;
        }

        // Delete the dynamic shares
        final SharedDeviceList shares = sess.getDynamicShareList();
        final Enumeration<SharedDevice> enm = shares.enumerateShares();
        while (enm.hasMoreElements()) {
            // Get the current share from the list
            final SharedDevice shr = (SharedDevice) enm.nextElement();

            // Close the shared device
            shr.getContext().CloseContext();
            if (LOGGER.isInfoEnabled() && hasDebug()) {
                LOGGER.info("Deleted dynamic share " + shr);
            }
        }
    }

    /**
     * Return the list of available shares.
     *
     * @param host
     *            String
     * @param sess
     *            SrvSession
     * @param allShares
     *            boolean
     * @return SharedDeviceList
     */
    @Override
    public SharedDeviceList getShareList(final String host, final SrvSession sess, final boolean allShares) {
        // Check that the filesystems configuration is valid
        if (m_filesysConfig == null) {
            return null;
        }

        // Make a copy of the global share list and add the per session dynamic shares
        final SharedDeviceList shrList = new SharedDeviceList(m_filesysConfig.getShares());
        if (sess != null && sess.hasDynamicShares()) {
            // Add the per session dynamic shares
            shrList.addShares(sess.getDynamicShareList());
        }

        // Remove unavailable shares from the list and return the list
        if (allShares == false) {
            shrList.removeUnavailableShares();
        }
        return shrList;
    }

    /**
     * Close the share mapper, release any resources.
     */
    @Override
    public void closeMapper() {
    }

    /**
     * configuration changed
     *
     * @param id
     *            int
     * @param config
     *            Serverconfiguration
     * @param newVal
     *            Object
     * @return int
     * @throws InvalidConfigurationException
     */
    @Override
    public int configurationChanged(final int id, final ServerConfiguration config, final Object newVal) throws InvalidConfigurationException {
        // Check if the filesystems configuration section has been added
        if (id == ConfigId.ConfigSection) {
            // Check if the section added is the filesystems config
            if (newVal instanceof FilesystemsConfigSection) {
                m_filesysConfig = (FilesystemsConfigSection) newVal;
            }
            // Return a dummy status
            return ConfigurationListener.StsAccepted;
        }

        // Return a dummy status
        return ConfigurationListener.StsIgnored;
    }
}
