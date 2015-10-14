/*
 * Copyright (C) 2006-2011 Alfresco Software Limited.
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

package org.alfresco.jlan.server.filesys.cache.hazelcast;

import org.alfresco.jlan.server.filesys.FileStatus;
import org.alfresco.jlan.server.filesys.cache.FileState;
import org.alfresco.jlan.server.filesys.cache.cluster.ClusterFileState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.IMap;

/**
 * Rename File State Task Class
 *
 * <p>
 *
 * @author gkspencer
 */
public class RenameStateTask extends RemoteStateTask<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RenameStateTask.class);

    // Serialization id
    private static final long serialVersionUID = 1L;

    // New file state key/path
    private String m_newKey;

    // Flag to indicate path is to a folder
    private boolean m_folder;

    /**
     * Default constructor
     */
    public RenameStateTask() {
    }

    /**
     * Class constructor
     *
     * @param mapName
     *            String
     * @param key
     *            String
     * @param newKey
     *            String
     * @param isFolder
     *            boolean
     * @param debug
     *            boolean
     * @param timingDebug
     *            boolean
     */
    public RenameStateTask(final String mapName, final String key, final String newKey, final boolean isFolder, final boolean debug,
            final boolean timingDebug) {
        super(mapName, key, true, true, debug, timingDebug);

        m_newKey = newKey;
        m_folder = isFolder;
    }

    /**
     * Run a remote task against a file state
     *
     * @param stateCache
     *            IMap<String, ClusterFileState>
     * @param fState
     *            ClusterFileState
     * @return Boolean
     * @exception Exception
     */
    @Override
    protected Boolean runRemoteTaskAgainstState(final IMap<String, ClusterFileState> stateCache, final ClusterFileState fState) throws Exception {
        if (hasDebug()) {
            LOGGER.debug("RenameStateTask: Rename from {} to {}", getKey(), m_newKey);
        }

        // Remove the existing file state from the cache, using the original name
        final ClusterFileState state = stateCache.remove(getKey());

        // Set the file status
        state.setFileStatusInternal(m_folder ? FileStatus.DirectoryExists : FileStatus.FileExists, FileState.ReasonNone);

        // Clear attributes from the renamed state
        state.removeAllAttributes();

        // Update the file state path and add it back to the cache using the new name
        state.setPathInternal(m_newKey);
        stateCache.put(state.getPath(), state);

        if (hasDebug()) {
            LOGGER.debug("Rename to {} successful, state={}", m_newKey, state);
        }

        // Return the rename status
        return Boolean.TRUE;
    }
}
