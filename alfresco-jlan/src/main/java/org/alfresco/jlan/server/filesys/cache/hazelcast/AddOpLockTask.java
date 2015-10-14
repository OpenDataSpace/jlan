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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.IMap;

/**
 * Add OpLock Remote Task Class
 *
 * <p>
 * Used to synchronize adding an oplock to a file state by executing on the remote node that owns the file state/key.
 *
 * @author gkspencer
 */
public class AddOpLockTask extends RemoteStateTask<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddOpLockTask.class);

    // Serialization id
    private static final long serialVersionUID = 1L;

    // Remote oplock details
    private RemoteOpLockDetails m_oplock;

    /**
     * Default constructor
     */
    public AddOpLockTask() {
    }

    /**
     * Class constructor
     *
     * @param mapName
     *            String
     * @param key
     *            String
     * @param oplock
     *            RemoteOpLockDetails
     * @param debug
     *            boolean
     * @param timingDebug
     *            boolean
     */
    public AddOpLockTask(final String mapName, final String key, final RemoteOpLockDetails oplock, final boolean debug, final boolean timingDebug) {
        super(mapName, key, true, false, debug, timingDebug);

        m_oplock = oplock;
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
            LOGGER.debug("AddOpLockTask: Add oplock={} to {}", m_oplock, fState);
        }

        // May throw an exception if there is an existing oplock on the file
        fState.setOpLock(m_oplock);

        // Return a success status
        return Boolean.TRUE;
    }
}
