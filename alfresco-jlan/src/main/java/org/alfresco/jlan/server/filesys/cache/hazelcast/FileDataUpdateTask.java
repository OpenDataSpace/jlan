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

import org.alfresco.jlan.server.filesys.cache.cluster.ClusterFileState;
import org.alfresco.jlan.server.filesys.cache.cluster.ClusterNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.IMap;

/**
 * File Data Update Remote Task Class
 *
 * <p>
 * Used to synchronize setting/clearing the file data update in progress details on a file state by executing on the remote node that owns the file state/key.
 *
 * @author gkspencer
 */
public class FileDataUpdateTask extends RemoteStateTask<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileDataUpdateTask.class);

    // Serialization id
    private static final long serialVersionUID = 1L;

    // Node that has the updated data
    private String m_updateNode;

    // Start of update or completed update
    private boolean m_startUpdate;

    /**
     * Default constructor
     */
    public FileDataUpdateTask() {
    }

    /**
     * Class constructor
     *
     * @param mapName
     *            String
     * @param key
     *            String
     * @param node
     *            ClusterNode
     * @param startUpdate
     *            boolean
     * @param debug
     *            boolean
     * @param timingDebug
     *            boolean
     */
    public FileDataUpdateTask(final String mapName, final String key, final ClusterNode node, final boolean startUpdate, final boolean debug,
            final boolean timingDebug) {
        super(mapName, key, true, false, debug, timingDebug);

        m_updateNode = node.getName();
        m_startUpdate = startUpdate;
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
            LOGGER.debug("FileDataUpdateTask: Update on node {} {} on {}", m_updateNode, m_startUpdate ? "started" : "completed", fState);
        }

        // Check if this is the start of the data update
        boolean updSts = false;
        if (m_startUpdate == true) {
            // Check if there is an existing data update on this file
            if (fState.hasDataUpdateInProgress()) {
                if (hasDebug()) {
                    LOGGER.debug("Existing data update on state={}", fState);
                }
            } else {
                // Set the node that has the updated file data
                fState.setDataUpdateNode(m_updateNode);
                updSts = true;
                if (hasDebug()) {
                    LOGGER.debug("File data update start on node={}, state={}", m_updateNode, fState);
                }
            }
        } else {
            // Check if the node matches the existing update node
            if (fState.hasDataUpdateInProgress()) {
                // Check the node
                if (fState.getDataUpdateNode().equals(m_updateNode) == false) {
                    if (hasDebug()) {
                        LOGGER.debug("Update is not the requesting node, node={}, update={}", m_updateNode, fState.getDataUpdateNode());
                    }
                } else {
                    // Clear the file data update, completed
                    fState.setDataUpdateNode(null);
                    updSts = true;
                    if (hasDebug()) {
                        LOGGER.debug("File data update complete on node=" + m_updateNode + ", state=" + fState);
                    }

                }
            }
        }

        // Return the updated file state
        return Boolean.valueOf(updSts);
    }
}
