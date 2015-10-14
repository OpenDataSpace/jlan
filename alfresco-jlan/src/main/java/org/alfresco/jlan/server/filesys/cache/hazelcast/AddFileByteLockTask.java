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

import org.alfresco.jlan.locking.FileLockList;
import org.alfresco.jlan.locking.LockConflictException;
import org.alfresco.jlan.server.filesys.cache.cluster.ClusterFileLock;
import org.alfresco.jlan.server.filesys.cache.cluster.ClusterFileState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.IMap;

/**
 * Add File Byte Range Lock Remote Task Class
 *
 * <p>
 * Used to synchronize adding a byte range lock to a file state by executing on the remote node that owns the file state/key.
 *
 * @author gkspencer
 */
public class AddFileByteLockTask extends RemoteStateTask<ClusterFileState> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddFileByteLockTask.class);

    // Serialization id
    private static final long serialVersionUID = 1L;

    // Byte range lock details
    private ClusterFileLock m_lock;

    /**
     * Default constructor
     */
    public AddFileByteLockTask() {
    }

    /**
     * Class constructor
     *
     * @param mapName
     *            String
     * @param key
     *            String
     * @param lock
     *            ClusterFileLock
     * @param debug
     *            boolean
     * @param timingDebug
     *            boolean
     */
    public AddFileByteLockTask(final String mapName, final String key, final ClusterFileLock lock, final boolean debug, final boolean timingDebug) {
        super(mapName, key, true, false, debug, timingDebug);

        m_lock = lock;
    }

    /**
     * Run a remote task against a file state
     *
     * @param stateCache
     *            IMap<String, ClusterFileState>
     * @param fState
     *            ClusterFileState
     * @return ClusterFileState
     * @exception Exception
     */
    @Override
    protected ClusterFileState runRemoteTaskAgainstState(final IMap<String, ClusterFileState> stateCache, final ClusterFileState fState) throws Exception {
        if (hasDebug()) {
            LOGGER.debug("AddFileByteLockTask: Add lock={} to {}", m_lock, fState);
        }

        // Check if there are any locks on the file
        if (fState.hasActiveLocks() == false) {
            // Add the lock
            fState.addLock(m_lock);
        } else {
            // Check for lock conflicts
            final FileLockList lockList = fState.getLockList();
            int idx = 0;

            while (idx < lockList.numberOfLocks()) {
                // Get the current file lock
                final ClusterFileLock curLock = (ClusterFileLock) lockList.getLockAt(idx++);

                // Check if the lock overlaps with the new lock
                if (curLock.hasOverlap(m_lock)) {
                    // Check the if the lock owner is the same
                    if (curLock.getProcessId() != m_lock.getProcessId() || curLock.getOwnerNode().equalsIgnoreCase(m_lock.getOwnerNode()) == false) {
                        if (hasDebug()) {
                            LOGGER.debug("AddLock Lock conflict with lock={}", curLock);
                        }

                        // Lock conflict
                        throw new LockConflictException();
                    }
                }
            }

            // Add the lock
            fState.addLock(m_lock);
        }

        // Return the updated file state
        return fState;
    }
}
