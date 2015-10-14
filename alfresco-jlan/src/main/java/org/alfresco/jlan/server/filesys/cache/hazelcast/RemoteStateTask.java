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

import java.io.Serializable;
import java.util.concurrent.Callable;

import org.alfresco.jlan.server.filesys.cache.cluster.ClusterFileState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;

/**
 * Remote File State Cache Task Class
 *
 * <p>
 * Base class for remote file state cache tasks.
 *
 * @author gkspencer
 */
public abstract class RemoteStateTask<T> implements Callable<T>, HazelcastInstanceAware, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteStateTask.class);

    // Serialization id
    private static final long serialVersionUID = 1L;

    // Task option flags
    public static final int TaskDebug = 0x0001;
    public static final int TaskLockState = 0x0002;
    public static final int TaskNoUpdate = 0x0004;
    public static final int TaskTiming = 0x0008;

    // Clustered map name and key
    private String m_mapName;
    private String m_keyName;

    // Hazelcast instance
    private transient HazelcastInstance m_hcInstance;

    // Task options
    private short m_taskOptions;

    // Task name
    private transient String m_taskName;

    /**
     * Default constructor
     */
    public RemoteStateTask() {
    }

    /**
     * Class constructor
     *
     * @param mapName
     *            String
     * @param key
     *            String
     * @param options
     *            int
     */
    public RemoteStateTask(final String mapName, final String key, final int options) {
        m_mapName = mapName;
        m_keyName = key;

        m_taskOptions = (short) options;
    }

    /**
     * Class constructor
     *
     * @param mapName
     *            String
     * @param key
     *            String
     * @param lockState
     *            boolean
     * @param noUpdate
     *            boolean
     * @param debug
     *            boolean
     * @param timingDebug
     *            boolean
     */
    public RemoteStateTask(final String mapName, final String key, final boolean lockState, final boolean noUpdate, final boolean debug,
            final boolean timingDebug) {
        m_mapName = mapName;
        m_keyName = key;

        if (lockState) {
            m_taskOptions += TaskLockState;
        }

        if (noUpdate) {
            m_taskOptions += TaskNoUpdate;
        }

        if (debug) {
            m_taskOptions += TaskDebug;
        }

        if (timingDebug) {
            m_taskOptions += TaskTiming;
        }
    }

    /**
     * Get the Hazelcast instance
     *
     * @return HazelcastInstance
     */
    public HazelcastInstance getHazelcastInstance() {
        return m_hcInstance;
    }

    /**
     * Set the Hazelcast instance
     *
     * @param hcInstance
     *            HazelcastInstance
     */
    @Override
    public void setHazelcastInstance(final HazelcastInstance hcInstance) {
        m_hcInstance = hcInstance;
    }

    /**
     * Return the clustered map name
     *
     * @return String
     */
    public final String getMapName() {
        return m_mapName;
    }

    /**
     * Return the file state key
     *
     * @return String
     */
    public final String getKey() {
        return m_keyName;
    }

    /**
     * Check if the specifed task option is enabled
     *
     * @param option
     *            int
     * @return boolean
     */
    public final boolean hasOption(final int option) {
        return (m_taskOptions & option) != 0 ? true : false;
    }

    /**
     * Check if debug output is enabled for this remote task
     *
     * @return boolean
     */
    public final boolean hasDebug() {
        return hasOption(TaskDebug);
    }

    /**
     * Check if the timing debug output is enabled for this remote task
     *
     * @return boolean
     */
    public final boolean hasTimingDebug() {
        return hasOption(TaskTiming);
    }

    /**
     * Get the task name
     *
     * @return String
     */
    public final String getTaskName() {
        if (m_taskName == null) {
            m_taskName = this.getClass().getSimpleName();
        }
        return m_taskName;
    }

    /**
     * Run the remote task
     */
    @Override
    public T call() throws Exception {
        long startTime = 0L;
        long lockTime = 0L;
        long unlockTime = 0L;

        if (hasTimingDebug()) {
            startTime = System.currentTimeMillis();
        }

        // Get the clustered cache
        final IMap<String, ClusterFileState> cache = getHazelcastInstance().getMap(getMapName());
        if (cache == null) {
            throw new Exception("Failed to find clustered map " + getMapName());
        }

        // Lock the file state if required, and load the current state
        if (hasOption(TaskLockState)) {
            long lockStart = 0L;
            if (hasTimingDebug()) {
                lockStart = System.currentTimeMillis();
            }

            // Lock the key
            cache.lock(getKey());
            if (hasTimingDebug()) {
                lockTime = System.currentTimeMillis() - lockStart;
            }
        }

        final ClusterFileState fState = cache.get(getKey());
        if (fState == null) {
            // Unlock the file state key and return an error
            if (hasOption(TaskLockState)) {
                cache.unlock(getKey());
            }

            throw new Exception("Failed to find file state for " + getKey());
        }

        // Run the task against the file state
        T retVal = null;
        try {
            // Run the remote task
            retVal = runRemoteTaskAgainstState(cache, fState);

            // Update the file state
            if (hasOption(TaskNoUpdate) == false) {
                cache.put(getKey(), fState);
                if (hasDebug()) {
                    LOGGER.debug("Remote task {} updated state={}", getTaskName(), fState);
                }
            }
        } finally {
            // Make sure the key is unlocked
            if (hasOption(TaskLockState)) {
                long lockEnd = 0L;
                if (hasTimingDebug()) {
                    lockEnd = System.currentTimeMillis();
                }

                // Unlock the key
                cache.unlock(getKey());

                if (hasTimingDebug()) {
                    unlockTime = System.currentTimeMillis() - lockEnd;
                }
            }

            if (hasTimingDebug()) {
                LOGGER.debug("Remote task {} executed in {}ms (lock {}ms, unlock {}ms)", getTaskName(), System.currentTimeMillis() - startTime, lockTime,
                        unlockTime);
            }
        }

        // Return the task result
        return retVal;
    }

    /**
     * Run a remote task against a file state
     *
     * @param stateCache
     *            IMap<String, ClusterFileState>
     * @param fState
     *            ClusterFileState
     * @return T
     * @exception Exception
     */
    protected abstract T runRemoteTaskAgainstState(IMap<String, ClusterFileState> stateCache, ClusterFileState fState) throws Exception;
}
