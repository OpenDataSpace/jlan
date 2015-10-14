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

package org.alfresco.jlan.server.filesys.cache;

import java.util.Enumeration;
import java.util.Hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File State Reaper Class
 *
 * <p>
 * FileStateCache objects register with the file state reaper to periodically check for expired file states.
 *
 * @author gkspencer
 */
public class FileStateReaper implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStateReaper.class);

    // Default expire check thread interval
    private static final long DEFAULT_EXPIRECHECK = 15000;

    // Wakeup interval for the expire file state checker thread
    private long m_expireInterval = DEFAULT_EXPIRECHECK;

    // File state checker thread
    private final Thread m_thread;

    // Shutdown request flag
    private boolean m_shutdown;

    // List of file state caches to be scanned for expired file states
    private final Hashtable<String, FileStateCache> m_stateCaches;

    // Debug output enabled
    private boolean m_debug;

    /**
     * Default constructor
     */
    public FileStateReaper() {
        // Create the reaper thread
        m_thread = new Thread(this);
        m_thread.setDaemon(true);
        m_thread.setName("FileStateReaper");
        m_thread.start();

        // Create the file state cache list
        m_stateCaches = new Hashtable<String, FileStateCache>();
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
     * Return the expired file state checker interval, in milliseconds
     *
     * @return long
     */
    public final long getCheckInterval() {
        return m_expireInterval;
    }

    /**
     * Set the expired file state checker interval, in milliseconds
     *
     * @param chkIntval
     *            long
     */
    public final void setCheckInterval(final long chkIntval) {
        m_expireInterval = chkIntval;
    }

    /**
     * Add a file state cache to the reaper list
     *
     * @param filesysName
     *            String
     * @param stateCache
     *            FileStateCache
     */
    public final void addStateCache(final String filesysName, final FileStateCache stateCache) {
        if (hasDebug()) {
            LOGGER.debug("Added file state cache for {}", filesysName);
        }

        m_stateCaches.put(filesysName, stateCache);

        // Inform the cache it can now start

        stateCache.stateCacheStarted();
    }

    /**
     * Remove a state cache from the reaper list
     *
     * @param filesysName
     *            String
     */
    public final void removeStateCache(final String filesysName) {
        final FileStateCache stateCache = m_stateCaches.remove(filesysName);
        if (hasDebug()) {
            LOGGER.debug("Removed file state table for {}", filesysName);
        }

        // Inform the cache to shut down
        stateCache.stateCacheShuttingDown();
    }

    /**
     * Expired file state checker thread
     */
    @Override
    public void run() {
        // Loop forever
        m_shutdown = false;
        while (m_shutdown == false) {
            // Sleep for the required interval
            try {
                Thread.sleep(getCheckInterval());
            } catch (final InterruptedException ex) {
            }

            // Check for shutdown
            if (m_shutdown == true) {
                if (hasDebug()) {
                    LOGGER.debug("FileStateReaper thread closing");
                }

                return;
            }

            // Check if there are any state caches registered
            if (m_stateCaches != null && m_stateCaches.size() > 0) {
                try {
                    // Loop through the registered file state caches and remove expired file states
                    final Enumeration<String> filesysNames = m_stateCaches.keys();
                    while (filesysNames.hasMoreElements()) {
                        // Get the current filesystem name and associated state cache
                        final String filesysName = filesysNames.nextElement();
                        final FileStateCache stateCache = m_stateCaches.get(filesysName);

                        // Check for expired file states
                        final int cnt = stateCache.removeExpiredFileStates();
                        if (hasDebug() && cnt > 0) {
                            LOGGER.debug("Expired {} file states for {}, cache={}", cnt, filesysName, stateCache.numberOfStates());
                        }
                    }
                } catch (final Exception ex) {
                    // Log errors if not shutting down
                    if (m_shutdown == false) {
                        LOGGER.warn(ex.getMessage(), ex);
                    }
                }
            }
        }
    }

    /**
     * Request the file state checker thread to shutdown
     */
    public final void shutdownRequest() {
        // Remove all the file state caches
        final Enumeration<String> filesysNames = m_stateCaches.keys();
        while (filesysNames.hasMoreElements()) {
            // Get the current filesystem name and remove the state cache
            final String filesysName = filesysNames.nextElement();
            removeStateCache(filesysName);
        }

        // Shutdown the checker thread
        m_shutdown = true;
        if (m_thread != null) {
            try {
                m_thread.interrupt();
            } catch (final Exception ex) {
            }
        }
    }
}
