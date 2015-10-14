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

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

import org.alfresco.jlan.debug.Debug;
import org.alfresco.jlan.locking.FileLock;
import org.alfresco.jlan.locking.FileLockList;
import org.alfresco.jlan.locking.LockConflictException;
import org.alfresco.jlan.locking.NotLockedException;
import org.alfresco.jlan.server.RequestPostProcessor;
import org.alfresco.jlan.server.config.CoreServerConfigSection;
import org.alfresco.jlan.server.config.InvalidConfigurationException;
import org.alfresco.jlan.server.config.ServerConfiguration;
import org.alfresco.jlan.server.filesys.AccessDeniedException;
import org.alfresco.jlan.server.filesys.DeferFailedException;
import org.alfresco.jlan.server.filesys.ExistingOpLockException;
import org.alfresco.jlan.server.filesys.FileAccessToken;
import org.alfresco.jlan.server.filesys.FileExistsException;
import org.alfresco.jlan.server.filesys.FileName;
import org.alfresco.jlan.server.filesys.FileOpenParams;
import org.alfresco.jlan.server.filesys.FileSharingException;
import org.alfresco.jlan.server.filesys.FileStatus;
import org.alfresco.jlan.server.filesys.NetworkFile;
import org.alfresco.jlan.server.filesys.NotifyChange;
import org.alfresco.jlan.server.filesys.cache.FileState;
import org.alfresco.jlan.server.filesys.cache.FileStateProxy;
import org.alfresco.jlan.server.filesys.cache.LocalFileStateProxy;
import org.alfresco.jlan.server.filesys.cache.cluster.ClusterFileLock;
import org.alfresco.jlan.server.filesys.cache.cluster.ClusterFileState;
import org.alfresco.jlan.server.filesys.cache.cluster.ClusterFileStateCache;
import org.alfresco.jlan.server.filesys.cache.cluster.ClusterInterface;
import org.alfresco.jlan.server.filesys.cache.cluster.ClusterNode;
import org.alfresco.jlan.server.filesys.cache.cluster.ClusterNodeList;
import org.alfresco.jlan.server.filesys.cache.cluster.PerNodeState;
import org.alfresco.jlan.server.locking.LocalOpLockDetails;
import org.alfresco.jlan.server.locking.OpLockDetails;
import org.alfresco.jlan.server.locking.OpLockManager;
import org.alfresco.jlan.server.thread.ThreadRequestPool;
import org.alfresco.jlan.smb.OpLock;
import org.alfresco.jlan.smb.SharingMode;
import org.alfresco.jlan.smb.server.SMBSrvPacket;
import org.alfresco.jlan.smb.server.SMBSrvSession;
import org.alfresco.jlan.smb.server.notify.NotifyChangeHandler;
import org.springframework.extensions.config.ConfigElement;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.partition.Partition;

/**
 * HazelCast Clustered File State Cache Class
 *
 * @author gkspencer
 */
public class HazelCastClusterFileStateCache extends ClusterFileStateCache
        implements ClusterInterface, MembershipListener, EntryListener<String, HazelCastClusterFileState>, MessageListener<ClusterMessage> {

    // Debug levels

    public static final int DebugStateCache = 0x00000001; // cache get/put/remove/rename/find
    public static final int DebugExpire = 0x00000002; // cache expiry
    public static final int DebugNearCache = 0x00000004; // near cache get/put/hits
    public static final int DebugOplock = 0x00000008; // oplock grant/release
    public static final int DebugByteLock = 0x00000010; // byte range lock/unlock
    public static final int DebugFileAccess = 0x00000020; // file access grant/release
    public static final int DebugMembership = 0x00000040; // cluster membership changes
    public static final int DebugCleanup = 0x00000080; // cleanup when node leaves cluster
    public static final int DebugPerNode = 0x00000100; // per node updates
    public static final int DebugClusterEntry = 0x00000200; // cluster entry updates
    public static final int DebugClusterMessage = 0x00000400; // cluster messaging
    public static final int DebugRemoteTask = 0x00000800; // remote tasks
    public static final int DebugRemoteTiming = 0x00001000; // remote task timing, key lock/unlock timing
    public static final int DebugRename = 0x00002000; // rename state
    public static final int DebugFileDataUpdate = 0x00004000; // file data updates
    public static final int DebugFileStatus = 0x00008000; // file status changes (exist/not exist)

    // Debug level names
    //
    // Note: Must match the order of the big flags

    private static final String[] _debugLevels = {"StateCache", "Expire", "NearCache", "Oplock", "ByteLock", "FileAccess", "Membership", "Cleanup", "PerNode",
            "ClusterEntry", "ClusterMessage", "RemoteTask", "RemoteTiming", "Rename", "FileDataUpdate", "FileStatus"};

    // Near-cache timeout values

    public static final long DefaultNearCacheTimeout = 5000L; // 5 seconds

    public static final long MinimumNearCacheTimeout = 3000L; // 3 seconds
    public static final long MaximumNearCacheTimeout = 120000L; // 2 minutes

    // Update mask to disable the state update post processor

    private final int DisableAllStateUpdates = -1;

    // Cluster name, map name in HazelCast, and messaging topic name

    private String m_clusterName;
    private String m_topicName;

    // Cluster configuration section

    private ClusterConfigSection m_clusterConfig;

    // HazelCast instance and cluster

    private HazelcastInstance m_hazelCastInstance;
    private Cluster m_cluster;

    // Clustered state cache

    private IMap<String, HazelCastClusterFileState> m_stateCache;

    // Pub/sub message topic used to receive oplock break requests from remote nodes

    private ITopic<ClusterMessage> m_clusterTopic;

    // Per node state cache, data that is not shared with the cluster, or cannot be shared

    private HashMap<String, PerNodeState> m_perNodeCache;

    // Near-cache of file states being accessed via this cluster node

    private ConcurrentHashMap<String, HazelCastClusterFileState> m_nearCache;
    private long m_nearCacheTimeout = DefaultNearCacheTimeout;

    // Thread pool from core config

    private ThreadRequestPool m_threadPool;

    // List of current cluster member nodes

    private ClusterNodeList m_nodes;

    // Local cluster node

    private ClusterNode m_localNode;

    // Local oplock manager

    private OpLockManager m_oplockManager;

    // Change notification handler, if configured for the filesystem

    private NotifyChangeHandler m_notifyHandler;

    // Option to send state updates for files/folders that do not exist

    private boolean m_sendNotExist = false;

    // Debug flags

    private int m_debugFlags;

    /**
     * Class constructor
     *
     */
    public HazelCastClusterFileStateCache() {

    }

    /**
     * Initialize the file state cache
     *
     * @param srvConfig
     *            ServerConfiguration
     * @throws InvalidConfigurationException
     */
    @Override
    public void initializeCache(final ConfigElement config, final ServerConfiguration srvConfig) throws InvalidConfigurationException {

        // Call the base class

        super.initializeCache(config, srvConfig);

        // Make sure there is a valid cluster configuration

        m_clusterConfig = (ClusterConfigSection) srvConfig.getConfigSection(ClusterConfigSection.SectionName);

        if (m_clusterConfig == null) {
            throw new InvalidConfigurationException("Cluster configuration not available");
        }

        // Check if the cluster name has been specfied

        ConfigElement elem = config.getChild("clusterName");
        if (elem != null && elem.getValue() != null) {

            // Set the cluster name

            m_clusterName = elem.getValue();

            // Validate the cluster name

            if (m_clusterName == null || m_clusterName.length() == 0) {
                throw new InvalidConfigurationException("Empty cluster name");
            }
        } else {
            throw new InvalidConfigurationException("Cluster name not specified");
        }

        // Check if the cluster topic name has been specfied

        elem = config.getChild("clusterTopic");
        if (elem != null && elem.getValue() != null) {

            // Set the cluster topic name

            m_topicName = elem.getValue();

            // Validate the oplocks name

            if (m_topicName == null || m_topicName.length() == 0) {
                throw new InvalidConfigurationException("Empty cluster topic name");
            }
        } else {
            throw new InvalidConfigurationException("Cluster topic name not specified");
        }

        // Create the near-cache, unless disabled via the configuration

        elem = config.getChild("nearCache");
        boolean useNearCache = true;

        if (elem != null) {

            // Check if the near cache has been disabled

            final String disableNear = elem.getAttribute("disable");

            if (Boolean.parseBoolean(disableNear) == true) {
                useNearCache = false;
            }

            // Check if the cache timeout value has been specified

            final String cacheTmo = elem.getAttribute("timeout");
            try {

                // Convert, validate, the cache timeout value

                m_nearCacheTimeout = Long.parseLong(cacheTmo) * 1000L;
                if (m_nearCacheTimeout < MinimumNearCacheTimeout || m_nearCacheTimeout > MaximumNearCacheTimeout) {
                    throw new InvalidConfigurationException(
                            "Near-cache timeout value out of valid range (" + MinimumNearCacheTimeout / 1000L + "-" + MaximumNearCacheTimeout / 1000L + ")");
                }
            } catch (final NumberFormatException ex) {
                throw new InvalidConfigurationException("Invalid near-cache timeout value specified, " + cacheTmo);
            }
        }

        // Create the near cache

        if (useNearCache == true) {
            m_nearCache = new ConcurrentHashMap<String, HazelCastClusterFileState>();
        }

        // Get the global thread pool

        final CoreServerConfigSection coreConfig = (CoreServerConfigSection) srvConfig.getConfigSection(CoreServerConfigSection.SectionName);
        m_threadPool = coreConfig.getThreadPool();

        // Set the cluster interface, embedded with the file state cache

        setCluster(this);

        // Create the per node state cache

        m_perNodeCache = new HashMap<String, PerNodeState>();

        // Check if debugging is enabled

        elem = config.getChild("cacheDebug");
        if (elem != null) {

            // Check for state cache debug flags

            String flags = elem.getAttribute("flags");
            int cacheDbg = 0;

            if (flags != null) {

                // Parse the flags

                flags = flags.toUpperCase();
                final StringTokenizer token = new StringTokenizer(flags, ",");

                while (token.hasMoreTokens()) {

                    // Get the current debug flag token

                    final String dbg = token.nextToken().trim();

                    // Find the debug flag name

                    int idx = 0;

                    while (idx < _debugLevels.length && _debugLevels[idx].equalsIgnoreCase(dbg) == false) {
                        idx++;
                    }

                    if (idx >= _debugLevels.length) {
                        throw new InvalidConfigurationException("Invalid state cache debug flag, " + dbg);
                    }

                    // Set the debug flag

                    cacheDbg += 1 << idx;
                }
            }

            // Set the cache debug flags

            m_debugFlags = cacheDbg;
        }
    }

    /**
     * Return the number of states in the cache
     *
     * @return int
     */
    @Override
    public int numberOfStates() {
        return m_stateCache != null ? m_stateCache.size() : 0;
    }

    /**
     * Enumerate the file state cache
     *
     * @return Enumeration<String>
     */
    public Enumeration<String> enumerateCache() {
        return null;
    }

    /**
     * Dump the state cache entries to the debug device
     *
     * @param dumpAttribs
     *            boolean
     */
    @Override
    public void dumpCache(final boolean dumpAttribs) {

        // Dump the file state cache entries to the specified stream

        if (m_stateCache.size() > 0) {
            Debug.println("++ HazelCastFileStateCache Entries:");
        }

        // Dump the local keys only

        final Set<String> localKeys = m_stateCache.localKeySet();

        // Check if there are any items in the cache

        if (localKeys.size() == 0) {
            return;
        }

        // Enumerate the file state cache and remove expired file state objects

        final Iterator<String> keysIter = localKeys.iterator();
        final long curTime = System.currentTimeMillis();

        while (keysIter.hasNext()) {
            final String fname = keysIter.next();
            final FileState state = m_stateCache.get(fname);

            Debug.println("++  " + fname + "(" + state.getSecondsToExpire(curTime) + ") : " + state.toString());

            // Check if the state attributes should be output

            if (dumpAttribs == true) {
                state.DumpAttributes();
            }
        }
    }

    /**
     * Return a file state proxy for the specified file state
     *
     * @param fstate
     *            FileState
     */
    @Override
    public FileStateProxy getFileStateProxy(final FileState fstate) {

        // Use a cluster proxy to avoid storing a reference to a copied file state object
        // Need to retrieve the file state on request, possibly from a local cache

        return new LocalFileStateProxy(fstate);
    }

    /**
     * Check if the near-cache is enabled
     *
     * @return boolean
     */
    public final boolean hasNearCache() {
        return m_nearCache != null ? true : false;
    }

    /**
     * Find the file state for the specified path
     *
     * @param path
     *            String
     * @return FileState
     */
    @Override
    public FileState findFileState(final String path) {
        final HazelCastClusterFileState fstate = m_stateCache.get(FileState.normalizePath(path, isCaseSensitive()));
        // Set the state cache the state belongs to, may have been fetched from the cluster
        if (fstate != null) {
            fstate.setStateCache(this);
        }
        return fstate;
    }

    /**
     * Find the file state for the specified path, and optionally create a new file state if not found
     *
     * @param path
     *            String
     * @param create
     *            boolean
     * @return FileState
     */
    @Override
    public FileState findFileState(final String path, final boolean create) {
        return findFileState(path, create, -1);
    }

    /**
     * Find the file state for the specified path, and optionally create a new file state if not found with the specified initial status
     *
     * @param path
     *            String
     * @param create
     *            boolean
     * @param status
     *            int
     * @return FileState
     */
    @Override
    public FileState findFileState(final String path, final boolean create, final int status) {

        // Normalize the path, used as the cache key

        final String normPath = FileState.normalizePath(path, isCaseSensitive());

        // If the near-cache is enabled check there first

        HazelCastClusterFileState state = getStateFromNearCache(normPath);

        // If the file state was not found in the near-cache, or the near-cache is not enabled, then check the clustered cache

        if (state == null) {
            state = m_stateCache.get(normPath);
        }

        // DEBUG

        if (hasDebugLevel(DebugStateCache)) {
            Debug.println("findFileState path=" + path + ", create=" + create + ", sts=" + FileStatus.asString(status) + ", state=" + state);
        }

        // Check if we should create a new file state

        if (state == null && create == true) {

            // Create a new file state

            state = new HazelCastClusterFileState(path, isCaseSensitive());

            // Set the file state timeout and add to the cache

            state.setExpiryTime(System.currentTimeMillis() + getFileStateExpireInterval());
            if (status != -1) {
                state.setFileStatus(status);
            }

            final HazelCastClusterFileState curState = m_stateCache.putIfAbsent(state.getPath(), state);

            if (curState != null) {

                // DEBUG

                if (hasDebugLevel(DebugStateCache)) {
                    Debug.println("Using existing state from putIfAbsent() returnedState=" + curState);
                    Debug.println("  newState=" + state);
                }

                // Switch to the existing file state

                state = curState;
            }

            // DEBUG

            if (hasDebugLevel(DebugStateCache)) {
                Debug.println("findFileState created state=" + state);
            }

            // Add the new state to the near-cache, if enabled

            if (hasNearCache()) {

                // Set the time the state was added to the near-cache

                state.setNearCacheTime();

                // Add to the near-cache

                m_nearCache.put(normPath, state);

                // DEBUG

                if (hasDebugLevel(DebugNearCache)) {
                    Debug.println("Added state to near-cache state=" + state);
                }
            }
        }

        // Set the state cache the state belongs to, may have been fetched from the cluster

        if (state != null) {
            state.setStateCache(this);
        }

        // Return the file state

        return state;
    }

    /**
     * Remove the file state for the specified path
     *
     * @param path
     *            String
     * @return FileState
     */
    @Override
    public FileState removeFileState(final String path) {

        // Remove the file state from the cache, and any associated per node data

        final String normPath = FileState.normalizePath(path, isCaseSensitive());

        final FileState state = m_stateCache.remove(normPath);
        m_perNodeCache.remove(normPath);

        // DEBUG

        if (hasDebugLevel(DebugStateCache)) {
            Debug.println("removeFileState path=" + path + ", state=" + state);
        }

        // Remvoe from the near-cache, if enabled

        if (hasNearCache()) {
            final HazelCastClusterFileState hcState = m_nearCache.remove(normPath);

            // DEBUG

            if (hasDebugLevel(DebugNearCache)) {
                Debug.println("Removed state from near-cache state=" + hcState);
            }
        }

        // Check if there is a state listener

        if (hasStateListener() && state != null) {
            getStateListener().fileStateClosed(state);
        }

        // Return the removed file state

        return state;
    }

    /**
     * Rename a file state, remove the existing entry, update the path and add the state back into the cache using the new path.
     *
     * @param newPath
     *            String
     * @param state
     *            FileState
     * @param isDir
     *            boolean
     */
    @Override
    public void renameFileState(final String newPath, final FileState state, final boolean isDir) {

        // DEBUG

        if (hasDebugLevel(DebugRename)) {
            Debug.println("Request rename via remote call, curPath=" + state.getPath() + ", newPath=" + newPath + ", isDir=" + isDir);
        }

        // Save the current path

        final String oldPath = state.getPath();

        // Normalize the new path

        final String newPathNorm = FileState.normalizePath(newPath, isCaseSensitive());

        // Rename the state via a remote call to the node that owns the file state

        final ExecutorService execService = m_hazelCastInstance.getExecutorService();
        final Callable<Boolean> callable = new RenameStateTask(getClusterName(), state.getPath(), newPathNorm, isDir, hasTaskDebug(), hasTaskTiming());
        final FutureTask<Boolean> renameStateTask = new DistributedTask<Boolean>(callable, state.getPath());

        execService.execute(renameStateTask);

        try {

            // Wait for the remote task to complete, check status

            if (renameStateTask.get().booleanValue() == Boolean.TRUE) {

                // Normalize the new path

                final String newNormPath = FileState.normalizePath(newPath, isCaseSensitive());

                // Update the per node data to the new path

                final PerNodeState perNode = m_perNodeCache.remove(oldPath);
                if (perNode != null) {
                    m_perNodeCache.put(newNormPath, perNode);
                }

                // Check if there is a near-cache entry

                if (hasNearCache()) {

                    // Check if the file state is in the near-cache

                    HazelCastClusterFileState hcState = m_nearCache.remove(oldPath);

                    if (hcState != null) {

                        // Update the state path

                        hcState.setPathInternal(newNormPath);

                        // Set the file/folder status

                        hcState.setFileStatusInternal(isDir ? FileStatus.DirectoryExists : FileStatus.FileExists, FileState.ReasonNone);

                        // Clear all attributes from the state

                        hcState.removeAllAttributes();

                        // Add the entry back using the new path

                        m_nearCache.put(hcState.getPath(), hcState);

                        // DEBUG

                        if (hasDebugLevel(DebugNearCache)) {
                            Debug.println("Rename near-cache entry, from=" + oldPath + ", to=" + hcState);
                        }
                    } else {

                        // Make sure we have a cluster file state

                        if (state instanceof HazelCastClusterFileState) {

                            // Set the time the state was added to the near-cache

                            hcState = (HazelCastClusterFileState) state;
                            hcState.setNearCacheTime();
                            hcState.setPathInternal(newNormPath);

                            // Add to the near-cache

                            m_nearCache.put(newNormPath, hcState);

                            // DEBUG

                            if (hasDebugLevel(DebugNearCache)) {
                                Debug.println("Added state to near-cache state=" + state + " (rename)");
                            }
                        }
                    }
                }

                // Notify cluster of the rename

                final StateRenameMessage stateRenameMsg = new StateRenameMessage(ClusterMessage.AllNodes, m_localNode, state.getPath(), newPath, isDir);
                m_clusterTopic.publish(stateRenameMsg);

                // DEBUG

                if (hasDebugLevel(DebugClusterMessage)) {
                    Debug.println("Sent file state rename to cluster, state=" + state + ", msg=" + stateRenameMsg);
                }
            } else {

                // Rename task failed

                throw new RuntimeException("Rename state task failed, state=" + state);
            }
        } catch (final ExecutionException ex) {

            // DEBUG

            if (hasDebugLevel(DebugRename)) {
                Debug.println("Error renaming state, fstate=" + state + ", newPath=" + newPath);
                Debug.println(ex);
            }

            // Problem executing the remote task

            throw new RuntimeException("Failed to rename state " + state.getPath(), ex);
        } catch (final InterruptedException ex2) {

            // DEBUG

            if (hasDebugLevel(DebugRename)) {
                Debug.println("Error renaming state, fstate=" + state + ", newPath=" + newPath);
                Debug.println(ex2);
            }

            // Problem executing the remote task

            throw new RuntimeException("Failed to rename state " + state.getPath(), ex2);
        }
    }

    /**
     * Remove all file states from the cache
     */
    @Override
    public void removeAllFileStates() {

        // Clear the near cache

        if (hasNearCache()) {
            m_nearCache.clear();
        }

        // Clear the per-node data cache

        m_perNodeCache.clear();
    }

    /**
     * Remove expired file states from the cache
     *
     * As the cache data is spread across the cluster we only expire file states that are stored on the local node.
     *
     * @return int
     */
    @Override
    public int removeExpiredFileStates() {

        // Only check the file states that are being stored on the local node

        if (m_stateCache == null) {
            return 0;
        }

        final Set<String> localKeys = m_stateCache.localKeySet();

        // Check if there are any items in the cache

        int expiredCnt = 0;

        if (localKeys.size() > 0) {

            // DEBUG

            if (hasDebugLevel(DebugExpire)) {
                Debug.println("Removing expired file states from local partition");
            }

            // Enumerate the file state cache and remove expired file state objects

            final Iterator<String> keysIter = localKeys.iterator();
            final long curTime = System.currentTimeMillis();

            int openCnt = 0;

            while (keysIter.hasNext()) {

                // Get the file state

                final ClusterFileState state = m_stateCache.get(keysIter.next());

                if (state != null && state.isPermanentState() == false) {

                    synchronized (state) {

                        // Check if the file state has expired and there are no open references to the
                        // file

                        if (state.hasExpired(curTime) && state.getOpenCount() == 0) {

                            // Check if there is a state listener

                            if (hasStateListener() && getStateListener().fileStateExpired(state) == true) {

                                // Remove the expired file state

                                final HazelCastClusterFileState hcState = m_stateCache.remove(state.getPath());

                                // Remove per node data for the expired file state

                                final PerNodeState perNode = m_perNodeCache.remove(state.getPath());

                                // DEBUG

                                if (hasDebugLevel(DebugExpire)) {
                                    Debug.println("++ Expired file state=" + hcState + ", perNode=" + perNode);
                                }

                                // Update the expired count

                                expiredCnt++;
                            }
                        } else if (state.getOpenCount() > 0) {
                            openCnt++;
                        }
                    }
                }
            }

            // DEBUG

            if (hasDebugLevel(DebugExpire)) { // && openCnt > 0) {
                Debug.println("++ Open files " + openCnt);
                dumpCache(false);
            }
        }

        // Expire states from the near-cache

        final boolean nearDebug = hasDebugLevel(DebugNearCache);
        final long checkTime = System.currentTimeMillis() - m_nearCacheTimeout;
        int nearExpireCnt = 0;

        if (hasNearCache() && m_nearCache.size() > 0) {

            // Iterate the near-cache

            final Iterator<String> nearIter = m_nearCache.keySet().iterator();

            while (nearIter.hasNext()) {

                // Get the current key and file state

                final String nearKey = nearIter.next();
                final HazelCastClusterFileState hcState = m_nearCache.get(nearKey);

                // Check if the near-cache entry has expired

                if (hcState.isStateValid() && hcState.getNearCacheLastAccessTime() < checkTime) {

                    // Remove the entry from the near-cache

                    m_nearCache.remove(nearKey);
                    nearExpireCnt++;

                    // Mark the file state as invalid

                    hcState.setStateValid(false);

                    // DEBUG

                    if (nearDebug) {
                        Debug.println("Removed from near-cache state=" + hcState);
                    }
                }
            }

            // DEBUG

            if (nearDebug && nearExpireCnt > 0) {
                Debug.println("Removed " + nearExpireCnt + " states from near-cache, " + m_nearCache.size() + " states remaining");
            }
        }

        // Return the count of expired file states that were removed

        return expiredCnt;
    }

    /**
     * Return the oplock details for a file, or null if there is no oplock
     *
     * @param fstate
     *            FileState
     * @return OpLockDetails
     */
    @Override
    public OpLockDetails getOpLock(final FileState fstate) {

        // Check if the file has an oplock

        OpLockDetails oplock = null;

        if (fstate.hasOpLock()) {

            // Check if the oplock is local to this node

            final PerNodeState perNode = m_perNodeCache.get(fstate.getPath());
            if (perNode != null && perNode.hasOpLock()) {
                oplock = perNode.getOpLock();
            }

            // Check if we found a local oplock, if not then must be owned by another node

            if (oplock == null) {
                oplock = fstate.getOpLock();

                if (oplock instanceof RemoteOpLockDetails) {
                    final RemoteOpLockDetails remoteOplock = (RemoteOpLockDetails) oplock;
                    final ClusterNode clNode = m_nodes.findNode(remoteOplock.getOwnerName());

                    if (clNode.isLocalNode()) {
                        oplock = null;

                        // Cleanup the near cache oplock

                        final HazelCastClusterFileState hcState = getStateFromNearCache(fstate.getPath());
                        if (hcState != null) {
                            hcState.clearOpLock();
                        }

                        // DEBUG

                        if (hasDebugLevel(DebugOplock)) {
                            Debug.println("Local oplock out of sync, cleared near cache for " + fstate);
                        }
                    }
                }
            }
        }

        // Return the oplock details

        return oplock;
    }

    /**
     * Add an oplock
     *
     * @param fstate
     *            FileState
     * @param oplock
     *            OpLockDetails
     * @param netFile
     *            NetworkFile
     * @exception ExistingOpLockException
     * @return boolean
     */
    @Override
    public boolean addOpLock(final FileState fstate, final OpLockDetails oplock, final NetworkFile netFile) throws ExistingOpLockException {

        // Make sure the oplock is a local oplock

        if (oplock instanceof LocalOpLockDetails == false) {
            throw new RuntimeException("Attempt to add non-local oplock to file state " + fstate.getPath());
        }

        // DEBUG

        if (hasDebugLevel(DebugOplock)) {
            Debug.println("Add oplock for state=" + fstate + ", oplock=" + oplock);
        }

        // Check if the oplock has already been granted by the file access check when the file was opened/created

        final ClusterFileState clState = (ClusterFileState) fstate;

        if (clState.hasLocalOpLock()) {

            // Check if the granted oplock matches the requested oplock details

            final LocalOpLockDetails grantedOplock = clState.getLocalOpLock();
            final LocalOpLockDetails reqOplock = (LocalOpLockDetails) oplock;

            if (reqOplock.getPath().equalsIgnoreCase(grantedOplock.getPath()) && reqOplock.getLockType() == grantedOplock.getLockType()
                    && reqOplock.getOwnerPID() == grantedOplock.getOwnerPID() && reqOplock.getOwnerTreeId() == grantedOplock.getOwnerTreeId()) {

                try {

                    // Switch to the new oplock, it contains the full details, the file id will be set later
                    // once the file open completes in the protocol layer

                    clState.clearLocalOpLock();
                    clState.setLocalOpLock(reqOplock);
                } catch (final ExistingOpLockException ex) {
                    Debug.println(ex);
                }

                // DEBUG

                if (hasDebugLevel(DebugOplock)) {
                    Debug.println("Oplock already granted via file access check, oplock=" + grantedOplock);
                }

                // Return a success status, oplock already granted, no need to make a remote call

                return true;
            }
        } else if (netFile.hasAccessToken()) {

            // Access token may indicate that the oplock is not available, no need to make a remote call

            final FileAccessToken token = netFile.getAccessToken();
            if (token != null && token instanceof HazelCastAccessToken) {
                final HazelCastAccessToken hcToken = (HazelCastAccessToken) token;
                if (hcToken.isOplockAvailable() == false) {

                    // DEBUG

                    if (hasDebugLevel(DebugOplock)) {
                        Debug.println("Oplock not available, via access token=" + hcToken);
                    }

                    // Oplock not available

                    return false;
                }
            }
        }

        // Create remote oplock details that can be stored in the cluster cache

        final RemoteOpLockDetails remoteOpLock = new RemoteOpLockDetails(getLocalNode(), oplock, this);

        // DEBUG

        if (hasDebugLevel(DebugOplock)) {
            Debug.println("Request oplock via remote call, remoteOplock=" + remoteOpLock);
        }

        // Add the oplock via a remote call to the node that owns the file state

        final ExecutorService execService = m_hazelCastInstance.getExecutorService();
        final Callable<Boolean> callable = new AddOpLockTask(getClusterName(), fstate.getPath(), remoteOpLock, hasTaskDebug(), hasTaskTiming());
        final FutureTask<Boolean> addOpLockTask = new DistributedTask<Boolean>(callable, fstate.getPath());

        execService.execute(addOpLockTask);

        boolean sts = false;

        try {

            // Wait for the remote task to complete, check status

            if (addOpLockTask.get().booleanValue() == Boolean.TRUE) {

                // Oplock added successfully, save the local oplock details in the per node data

                clState.setLocalOpLock((LocalOpLockDetails) oplock);

                // Update the near-cache

                if (hasNearCache()) {

                    // Check if the file state is in the near-cache

                    final HazelCastClusterFileState hcState = getStateFromNearCache(fstate.getPath());
                    if (hcState != null) {

                        // Add the remote oplock

                        hcState.setOpLock(remoteOpLock);

                        // DEBUG

                        if (hasDebugLevel(DebugNearCache)) {
                            Debug.println("Added oplock to near-cache state=" + hcState);
                        }
                    }
                }

                // Indicate the oplock was added successfully

                sts = true;
            }
        } catch (final ExecutionException ex) {

            // DEBUG

            if (hasDebugLevel(DebugOplock)) {
                Debug.println("Error adding oplock, fstate=" + fstate + ", oplock=" + oplock);
                Debug.println(ex);
            }

            // Problem executing the remote task

            throw new ExistingOpLockException("Failed to execute remote oplock add on " + fstate.getPath(), ex);
        } catch (final InterruptedException ex2) {

            // DEBUG

            if (hasDebugLevel(DebugOplock)) {
                Debug.println("Error adding oplock, fstate=" + fstate + ", oplock=" + oplock);
                Debug.println(ex2);
            }

            // Problem executing the remote task

            throw new ExistingOpLockException("Failed to execute remote oplock add on " + fstate.getPath(), ex2);
        }

        // Return the add oplock status

        return sts;
    }

    /**
     * Clear an oplock
     *
     * @param fstate
     *            FileState
     */
    @Override
    public void clearOpLock(final FileState fstate) {

        // Access the cluster file state

        final ClusterFileState clState = (ClusterFileState) fstate;

        // DEBUG

        if (hasDebugLevel(DebugOplock)) {
            Debug.println("Clear oplock for state=" + fstate);
        }

        // Remove the oplock from local oplock list

        final PerNodeState perNode = m_perNodeCache.get(clState.getPath());

        if (perNode != null && perNode.hasOpLock()) {

            // Remove the oplock using a remote call to the node that owns the file state

            final ExecutorService execService = m_hazelCastInstance.getExecutorService();
            final Callable<Boolean> callable = new RemoveOpLockTask(getClusterName(), fstate.getPath(), hasTaskDebug(), hasTaskTiming());
            final FutureTask<Boolean> removeOpLockTask = new DistributedTask<Boolean>(callable, fstate.getPath());

            execService.execute(removeOpLockTask);

            try {

                // Wait for the remote task to complete

                removeOpLockTask.get();

                // Update the near-cache

                if (hasNearCache()) {

                    // Check if the file state is in the near-cache

                    final HazelCastClusterFileState hcState = getStateFromNearCache(fstate.getPath());
                    if (hcState != null) {

                        // Remove the remote oplock details

                        hcState.clearOpLock();

                        // DEBUG

                        if (hasDebugLevel(DebugNearCache)) {
                            Debug.println("Cleared oplock from near-cache state=" + hcState);
                        }
                    }
                }
            } catch (final Exception ex) {

                // Problem executing the remote task

                Debug.println(ex, Debug.Error);
            }

            // Inform cluster nodes that an oplock has been released

            final OpLockMessage oplockMsg = new OpLockMessage(ClusterMessage.AllNodes, ClusterMessageType.OpLockBreakNotify, clState.getPath());
            m_clusterTopic.publish(oplockMsg);
        } else if (hasDebugLevel(DebugOplock)) {
            Debug.println("No local oplock found for " + fstate);
        }
    }

    /**
     * Create a file lock object
     *
     * @param file
     *            NetworkFile
     * @param offset
     *            long
     * @param len
     *            long
     * @param pid
     *            int
     */
    @Override
    public FileLock createFileLockObject(final NetworkFile file, final long offset, final long len, final int pid) {

        // Create a lock object to represent the file lock

        return new ClusterFileLock(m_localNode, offset, len, pid);
    }

    /**
     * Check if there are active locks on this file
     *
     * @param fstate
     *            FileState
     * @return boolean
     */
    @Override
    public boolean hasActiveLocks(final FileState fstate) {
        return fstate.hasActiveLocks();
    }

    /**
     * Add a lock to this file
     *
     * @param fstate
     *            FileState
     * @param lock
     *            FileLock
     * @exception LockConflictException
     */
    @Override
    public void addLock(final FileState fstate, final FileLock lock) throws LockConflictException {

        // Make sure the lock is a cluster lock

        if (lock instanceof ClusterFileLock == false) {
            throw new RuntimeException("Attempt to add non-cluster byte lock to file state " + fstate.getPath());
        }

        // DEBUG

        if (hasDebugLevel(DebugByteLock)) {
            Debug.println("Add byte lock for state=" + fstate + ", lock=" + lock);
        }

        // Add the oplock via a remote call to the node that owns the file state

        final ExecutorService execService = m_hazelCastInstance.getExecutorService();
        final Callable<ClusterFileState> callable = new AddFileByteLockTask(getClusterName(), fstate.getPath(), (ClusterFileLock) lock,
                hasDebugLevel(DebugByteLock), hasTaskTiming());
        final FutureTask<ClusterFileState> addLockTask = new DistributedTask<ClusterFileState>(callable, fstate.getPath());

        execService.execute(addLockTask);

        try {

            // Wait for the remote task to complete

            final ClusterFileState clState = addLockTask.get();

            // Update the near-cache with the new state

            updateNearCacheState(clState);
        } catch (final ExecutionException ex) {

            // DEBUG

            if (hasDebugLevel(DebugByteLock)) {
                Debug.println("Error adding byte lock, fstate=" + fstate + ", lock=" + lock);
                Debug.println(ex);
            }

            // Problem executing the remote task

            throw new LockConflictException("Failed to execute remote lock add on " + fstate.getPath(), ex);
        } catch (final InterruptedException ex2) {

            // DEBUG

            if (hasDebugLevel(DebugByteLock)) {
                Debug.println("Error adding byte lock, fstate=" + fstate + ", lock=" + lock);
                Debug.println(ex2);
            }

            // Problem executing the remote task

            throw new LockConflictException("Failed to execute remote lock add on " + fstate.getPath(), ex2);
        }
    }

    /**
     * Remove a lock on this file
     *
     * @param fstate
     *            FileState
     * @param lock
     *            FileLock
     * @exception NotLockedException
     */
    @Override
    public void removeLock(final FileState fstate, final FileLock lock) throws NotLockedException {

        // Make sure the lock is a cluster lock

        if (lock instanceof ClusterFileLock == false) {
            throw new RuntimeException("Attempt to remove non-cluster byte lock from file state " + fstate.getPath());
        }

        // DEBUG

        if (hasDebugLevel(DebugByteLock)) {
            Debug.println("Remove byte lock for state=" + fstate + ", lock=" + lock);
        }

        // Add the oplock via a remote call to the node that owns the file state

        final ExecutorService execService = m_hazelCastInstance.getExecutorService();
        final Callable<ClusterFileState> callable = new RemoveFileByteLockTask(getClusterName(), fstate.getPath(), (ClusterFileLock) lock,
                hasDebugLevel(DebugByteLock), hasTaskTiming());
        final FutureTask<ClusterFileState> removeLockTask = new DistributedTask<ClusterFileState>(callable, fstate.getPath());

        execService.execute(removeLockTask);

        try {

            // Wait for the remote task to complete

            final ClusterFileState clState = removeLockTask.get();

            // Update the near-cache with the new state

            updateNearCacheState(clState);
        } catch (final ExecutionException ex) {

            // DEBUG

            if (hasDebugLevel(DebugByteLock)) {
                Debug.println("Error removing byte lock, fstate=" + fstate + ", lock=" + lock);
                Debug.println(ex);
            }

            // Problem executing the remote task

            throw new NotLockedException("Failed to execute remote unlock add on " + fstate.getPath(), ex);
        } catch (final InterruptedException ex2) {

            // DEBUG

            if (hasDebugLevel(DebugByteLock)) {
                Debug.println("Error removing byte lock, fstate=" + fstate + ", lock=" + lock);
                Debug.println(ex2);
            }

            // Problem executing the remote task

            throw new NotLockedException("Failed to execute remote unlock add on " + fstate.getPath(), ex2);
        }
    }

    /**
     * Start the cluster
     *
     * @throws Exception
     */
    @Override
    public void startCluster() throws Exception {

        // DEBUG

        if (Debug.EnableDbg && hasDebug()) {
            Debug.println("Starting cluster, name=" + getClusterName());
        }

        // Create/join a cluster using the specified configuration

        m_hazelCastInstance = m_clusterConfig.getHazelcastInstance();
        m_cluster = m_hazelCastInstance.getCluster();

        // Build the initial cluster node list

        rebuildClusterNodeList();

        // Add a listener to receive cluster membership events

        m_cluster.addMembershipListener(this);

        // Create the clustered state cache map

        m_stateCache = m_hazelCastInstance.getMap(getClusterName());
        if (m_stateCache == null) {
            throw new Exception("Failed to initialize state cache, " + getClusterName());
        }

        // Create the pub/sub message topic for cluster messages

        m_clusterTopic = m_hazelCastInstance.getTopic(m_topicName);
        if (m_clusterTopic == null) {
            throw new Exception("Failed to initialize cluster topic, " + m_topicName);
        }

        // Signal that the cluster cache is running, this will mark the filesystem as available

        if (m_stateCache != null && m_clusterTopic != null) {

            // Add a listener to receive cluster cache entry events

            m_stateCache.addEntryListener(this, false);

            // Add a listener to receive cluster messages via the topic

            m_clusterTopic.addMessageListener(this);

            // Indicate that the cluster is running

            getStateCache().clusterRunning();
        }
    }

    /**
     * Shutdown the cluster
     */
    @Override
    public void shutdownCluster() throws Exception {

        // DEBUG

        if (Debug.EnableDbg && hasDebug()) {
            Debug.println("Shutting cluster, name=" + getClusterName());
        }

        // Hazelcast will be shutdown when the cluster configuration section is closed, it may be shared
        // by multiple components/filessytems.
    }

    /**
     * Request an oplock break
     *
     * @param path
     *            String
     * @param oplock
     *            OpLockDetails
     * @param sess
     *            SMBSrvSession
     * @param pkt
     *            SMBSrvPacket
     * @exception IOException
     * @exception DeferFailedException
     */
    @Override
    public void requestOplockBreak(final String path, final OpLockDetails oplock, final SMBSrvSession sess, final SMBSrvPacket pkt)
            throws IOException, DeferFailedException {

        // Check if the oplock is owned by the local node

        final String normPath = FileState.normalizePath(path, isCaseSensitive());
        final PerNodeState perNode = m_perNodeCache.get(normPath);

        if (perNode != null && perNode.hasOpLock()) {

            // Get the local oplock

            final LocalOpLockDetails localOpLock = perNode.getOpLock();

            // Save the session/packet details so the request can be continued once the client owning the
            // oplock releases it

            localOpLock.addDeferredSession(sess, pkt);

            // DEBUG

            if (hasDebugLevel(DebugOplock)) {
                Debug.println("Request oplock break, path=" + path + ", via local oplock=" + localOpLock);
            }

            // Request an oplock break

            localOpLock.requestOpLockBreak();
        } else if (oplock instanceof RemoteOpLockDetails) {

            // DEBUG

            if (hasDebugLevel(DebugOplock)) {
                Debug.println("Request oplock break, path=" + path + ", via remote oplock=" + oplock);
            }

            // Remote oplock, get the oplock owner cluster member details

            final RemoteOpLockDetails remoteOplock = (RemoteOpLockDetails) oplock;
            final ClusterNode clNode = m_nodes.findNode(remoteOplock.getOwnerName());

            if (clNode == null) {

                // DEBUG

                if (hasDebugLevel(DebugOplock)) {
                    Debug.println("Cannot find node details for " + remoteOplock.getOwnerName());
                }

                // Cannot find the node that owns the oplock

                throw new IOException("Cannot find remote oplock node details for " + remoteOplock.getOwnerName());
            } else if (clNode.isLocalNode()) {

                // Should not be the local node

                throw new IOException("Attempt to send remote oplock break to local node, path=" + path);
            }

            // Make sure the associated state cache is set for the remote oplock

            remoteOplock.setStateCache(this);

            // Save the session/packet details so the request can be continued once the client owning the
            // oplock releases it

            remoteOplock.addDeferredSession(sess, pkt);

            // Send an oplock break request to the cluster

            final OpLockMessage oplockMsg = new OpLockMessage(clNode.getName(), ClusterMessageType.OpLockBreakRequest, normPath);
            m_clusterTopic.publish(oplockMsg);
        } else if (hasDebugLevel(DebugOplock)) {
            Debug.println("Unable to send oplock break, oplock=" + oplock);
        }
    }

    /**
     * Change an oplock type
     *
     * @param oplock
     *            OpLockDetails
     * @param newTyp
     *            int
     */
    @Override
    public void changeOpLockType(final OpLockDetails oplock, final int newTyp) {

        // DEBUG

        if (hasDebugLevel(DebugOplock)) {
            Debug.println("Change oplock type to=" + OpLock.getTypeAsString(newTyp) + " for oplock=" + oplock);
        }

        // Run the file access checks via the node that owns the file state

        final String normPath = FileState.normalizePath(oplock.getPath(), isCaseSensitive());

        final ExecutorService execService = m_hazelCastInstance.getExecutorService();
        final Callable<Integer> callable = new ChangeOpLockTypeTask(getClusterName(), normPath, newTyp, hasTaskDebug(), hasTaskTiming());
        final FutureTask<Integer> changeOpLockTask = new DistributedTask<Integer>(callable, oplock.getPath());

        execService.execute(changeOpLockTask);

        try {

            // Wait for the remote task to complete, get the returned oplock type

            final Integer newOplockType = changeOpLockTask.get();

            // Check that the update was successful

            if (newOplockType.intValue() == newTyp) {

                // Check if this node owns the oplock

                final PerNodeState perNode = m_perNodeCache.get(normPath);

                if (perNode != null && perNode.hasOpLock()) {

                    // Get the local oplock

                    final LocalOpLockDetails localOpLock = perNode.getOpLock();

                    // Update the local oplock type

                    localOpLock.setLockType(newTyp);
                }

                // Check if the near cache has a copy of the oplock

                if (hasNearCache()) {

                    // Check if we have the state cached in the near-cache

                    final HazelCastClusterFileState hcState = getStateFromNearCache(normPath);
                    if (hcState != null) {

                        // Check if the near cache copy has the oplock details

                        if (hcState.hasOpLock()) {

                            // Update the near cache oplock details

                            hcState.getOpLock().setLockType(newTyp);

                            // DEBUG

                            if (hasDebugLevel(DebugNearCache)) {
                                Debug.println("Near-cache updated oplock type to=" + OpLock.getTypeAsString(newTyp) + ", nearState=" + hcState);
                            }
                        } else {

                            // Out of sync near cache state, mark it as invalid

                            hcState.setStateValid(false);

                            // DEBUG

                            if (hasDebugLevel(DebugNearCache)) {
                                Debug.println("Near-cache no oplock, marked as invalid, nearState=" + hcState);
                            }
                        }
                    }
                }

                // Inform all nodes of the oplock type change

                final OpLockMessage oplockMsg = new OpLockMessage(ClusterMessage.AllNodes, ClusterMessageType.OplockTypeChange, normPath);
                m_clusterTopic.publish(oplockMsg);
            } else {

                // DEBUG

                if (hasDebugLevel(DebugOplock)) {
                    Debug.println("Failed to change oplock type, no oplock on file state, path=" + oplock.getPath());
                }
            }
        } catch (final Exception ex) {

            // DEBUG

            if (hasDebugLevel(DebugOplock)) {
                Debug.println("Error changing oplock type to=" + OpLock.getTypeAsString(newTyp) + ", for oplock=" + oplock);
                Debug.println(ex);
            }
        }
    }

    /**
     * Cluster member added
     *
     * @param membershipEvent
     *            MembershipEvent
     */
    @Override
    public void memberAdded(final MembershipEvent membershipEvent) {

        // DEBUG

        if (Debug.EnableDbg && hasDebugLevel(DebugMembership)) {
            Debug.println("Cluster added member " + membershipEvent.getMember());
        }

        // Rebuild the cluster node list

        rebuildClusterNodeList();
    }

    /**
     * Cluster member removed
     *
     * @param membershipEvent
     *            MembershipEvent
     */
    @Override
    public void memberRemoved(final MembershipEvent membershipEvent) {

        // DEBUG

        if (Debug.EnableDbg && hasDebugLevel(DebugMembership)) {
            Debug.println("Cluster removed member " + membershipEvent.getMember());
        }

        // Rebuild the cluster node list

        rebuildClusterNodeList();

        // Remove file state resources owned by the node that has just left the cluster, such as
        // oplocks, byte range locks

        removeMemberData(membershipEvent.getMember());
    }

    /**
     * Rebuild the cluster node list
     */
    private synchronized final void rebuildClusterNodeList() {

        // DEBUG

        if (Debug.EnableDbg && hasDebugLevel(DebugMembership)) {
            Debug.println("Rebuilding cluster node list");
        }

        // Get the current node list

        final ClusterNodeList curList = getNodeList();

        // Create a new list

        final ClusterNodeList newList = new ClusterNodeList();

        // Get the current cluster member list

        final Set<Member> members = m_cluster.getMembers();
        final Iterator<Member> iterMembers = members.iterator();
        int nodeId = 1;

        while (iterMembers.hasNext()) {

            // Get the next cluster member

            final Member curMember = iterMembers.next();
            ClusterNode clNode = null;
            final String clName = curMember.getInetSocketAddress().toString();

            if (curList != null && curList.numberOfNodes() > 0) {

                // Check if the node exists in the current list

                clNode = curList.findNode(clName);
            }

            // Create a new node if not found in the current list

            if (clNode == null) {
                clNode = new HazelCastClusterNode(clName, nodeId, this, curMember);
            } else {
                clNode.setPriority(nodeId);
            }

            // Add the node to the new list

            newList.addNode(clNode);

            // Check for the local node

            if (clNode.isLocalNode()) {
                setLocalNode(clNode);
            }

            // Update the node id

            nodeId++;
        }

        // Update the list of nodes

        setNodeList(newList);

        // DEBUG

        if (Debug.EnableDbg && hasDebugLevel(DebugMembership)) {
            Debug.println("  New member list: " + newList);
        }
    }

    /**
     * Remove cluster cache data that is owned by the specified cluster member as the member has left the cluster (such as file locks and oplocks).
     *
     * As the cache data is spread across the cluster we remove data that is on the file states that are stored on the local node.
     *
     * @param member
     *            Member
     * @return int
     */
    protected int removeMemberData(final Member member) {

        // Only check the file states that are being stored on the local node

        if (m_stateCache == null) {
            return 0;
        }

        final Set<String> localKeys = m_stateCache.localKeySet();

        // Check if there are any items in the cache

        if (localKeys.size() == 0) {
            return 0;
        }

        // DEBUG

        if (hasDebugLevel(DebugCleanup)) {
            Debug.println("Removing state data for member " + member);
        }

        // Get the member name

        final String memberName = member.toString();

        // Enumerate the file state cache and remove expired file state objects

        int stateCnt = 0;
        final Iterator<String> keysIter = localKeys.iterator();

        while (keysIter.hasNext()) {

            // Get the file state

            HazelCastClusterFileState state = m_stateCache.get(keysIter.next());

            // Check if the node had the file open as the primary owner

            final String primaryOwner = (String) state.getPrimaryOwner();

            if (primaryOwner != null && primaryOwner.equals(memberName)) {

                // Reduce the file open count

                if (state.getOpenCount() > 0) {
                    state.decrementOpenCount();
                }

                // Reset the shared access mode, and clear the primary owner

                state.setSharedAccess(SharingMode.READWRITEDELETE);
                state.setPrimaryOwner(null);

                // DEBUG

                if (hasDebugLevel(DebugCleanup)) {
                    Debug.println("  Cleared primary owner, state=" + state);
                }
            }

            // Check if there are any byte range locks owned by the member

            if (state.hasActiveLocks()) {

                // Check the lock list, without locking the file state on the first pass. If there are locks
                // belonging to the member then lock and remove them in a second pass

                FileLockList lockList = state.getLockList();
                int lockCnt = 0;

                for (int idx = 0; idx < lockList.numberOfLocks(); idx++) {
                    final ClusterFileLock curLock = (ClusterFileLock) lockList.getLockAt(idx);
                    if (curLock.getOwnerNode().equalsIgnoreCase(memberName)) {
                        lockCnt++;
                    }
                }

                // DEBUG

                if (hasDebugLevel(DebugCleanup) && lockCnt > 0) {
                    Debug.println("  Removing " + lockCnt + " file locks, state=" + state);
                }

                // If there are locks owned by the member then lock the file state and remove them in a second
                // pass of the lock list. We must get the file state again after locking as it might have been updated.

                m_stateCache.lock(state.getPath());
                state = m_stateCache.get(state.getPath());

                lockList = state.getLockList();

                int idx = 0;

                while (idx < lockList.numberOfLocks()) {

                    // Get the current lock, if is owned by the member then remove it

                    final ClusterFileLock curLock = (ClusterFileLock) lockList.getLockAt(idx);
                    if (curLock.getOwnerNode().equalsIgnoreCase(memberName)) {
                        lockList.removeLockAt(idx);
                    } else {
                        idx++;
                    }
                }

                // Check the oplock whilst we have the state locked

                if (state.hasOpLock()) {

                    // Get the oplock details

                    final RemoteOpLockDetails oplock = (RemoteOpLockDetails) state.getOpLock();
                    if (oplock.getOwnerName().equalsIgnoreCase(memberName)) {

                        // Remove the oplock

                        state.clearOpLock();

                        // DEBUG

                        if (hasDebugLevel(DebugCleanup)) {
                            Debug.println("  And removing oplock");
                        }
                    }
                }

                // Update the state in the cache, and unlock

                m_stateCache.put(state.getPath(), state);
                m_stateCache.unlock(state.getPath());

                // Increment the updated state count

                stateCnt++;
            }

            // Check if the state has an oplock owned by the member

            if (state.hasOpLock()) {

                // Get the oplock details

                final RemoteOpLockDetails oplock = (RemoteOpLockDetails) state.getOpLock();
                if (oplock.getOwnerName().equalsIgnoreCase(memberName)) {

                    // DEBUG

                    if (hasDebugLevel(DebugCleanup)) {
                        Debug.println("  Removing oplock, state=" + state);
                    }

                    // Lock the file state and reload it, may have changed

                    m_stateCache.lock(state.getPath());
                    state = m_stateCache.get(state.getPath());

                    // Clear the oplock

                    state.clearOpLock();

                    // Update the state in the cache, and unlock

                    m_stateCache.put(state.getPath(), state);
                    m_stateCache.unlock(state.getPath());

                    // Increment the updated state count

                    stateCnt++;
                }
            }
        }

        // Return the count of file states that were updated

        return stateCnt;
    }

    /**
     * Return the per node state for a file state, and optionally create a new per node state
     *
     * @param fState
     *            ClusterFileState
     * @param createState
     *            boolean
     * @return PerNodeState
     */
    @Override
    public PerNodeState getPerNodeState(final ClusterFileState fState, final boolean createState) {
        PerNodeState perNode = m_perNodeCache.get(fState.getPath());
        if (perNode == null && createState == true) {
            perNode = new PerNodeState();
            m_perNodeCache.put(fState.getPath(), perNode);
        }

        return perNode;
    }

    /**
     * Return the per node state for a file path, and optionally create a new per node state
     *
     * @param path
     *            String
     * @param createState
     *            boolean
     * @return PerNodeState
     */
    @Override
    public PerNodeState getPerNodeState(final String path, final boolean createState) {
        final String normPath = FileState.normalizePath(path, isCaseSensitive());
        PerNodeState perNode = m_perNodeCache.get(normPath);
        if (perNode == null && createState == true) {
            perNode = new PerNodeState();
            m_perNodeCache.put(normPath, perNode);
        }

        return perNode;
    }

    /**
     * Grant the required file access
     *
     * @param params
     *            FileOpenParams
     * @param fstate
     *            FileState
     * @param fileSts
     *            int
     * @return FileAccessToken
     * @exception FileSharingException
     * @exception AccessDeniedException
     * @exception FileExistsException
     */
    @Override
    public FileAccessToken grantFileAccess(final FileOpenParams params, final FileState fstate, final int fileSts)
            throws FileSharingException, AccessDeniedException, FileExistsException {

        // DEBUG

        if (hasDebugLevel(DebugFileAccess)) {
            Debug.println("Grant file access for state=" + fstate + ", params=" + params + ", fileSts=" + FileStatus.asString(fileSts));
        }

        // Send a subset of the file open parameters to the remote task

        final GrantAccessParams grantParams = new GrantAccessParams(getLocalNode(), params, fileSts);

        // Run the file access checks via the node that owns the file state

        final ExecutorService execService = m_hazelCastInstance.getExecutorService();
        final Callable<FileAccessToken> callable = new GrantFileAccessTask(getClusterName(), fstate.getPath(), grantParams, hasTaskDebug(), hasTaskTiming());
        final FutureTask<FileAccessToken> grantAccessTask = new DistributedTask<FileAccessToken>(callable, fstate.getPath());

        execService.execute(grantAccessTask);

        HazelCastAccessToken accessToken = null;

        try {

            // Wait for the remote task to complete, get the returned access token

            accessToken = (HazelCastAccessToken) grantAccessTask.get();

            // Set the associated path for the access token, and mark as not released

            accessToken.setNetworkFilePath(params.getPath());
            accessToken.setReleased(false);

            // Check if an oplock was also granted during the file access check

            if (accessToken.getOpLockType() != OpLock.TypeNone) {

                // Create the local oplock details

                final LocalOpLockDetails localOplock = new LocalOpLockDetails(accessToken.getOpLockType(), params.getFullPath(),
                        (SMBSrvSession) params.getSession(), params.getProcessId(), params.getTreeId(), fileSts == FileStatus.DirectoryExists ? true : false);

                // Save the local oplock, in the per node data

                final ClusterFileState clState = (ClusterFileState) fstate;
                clState.setLocalOpLock(localOplock);

                // Update the near-cache

                if (hasNearCache()) {

                    // Check if the file state is in the near-cache

                    final HazelCastClusterFileState hcState = getStateFromNearCache(fstate.getPath());
                    if (hcState != null) {

                        // Create a remote oplock

                        final RemoteOpLockDetails remoteOpLock = new RemoteOpLockDetails(getLocalNode(), localOplock, this);

                        // Add the remote oplock, set the file open count, must be one as oplock was granted

                        hcState.setOpLock(remoteOpLock);
                        hcState.setOpenCount(1);

                        // DEBUG

                        if (hasDebugLevel(DebugNearCache)) {
                            Debug.println("Added oplock to near-cache (via grant access) state=" + hcState);
                        }
                    }
                }
            } else if (hasNearCache()) {

                // Check if the file state is in the near-cache

                final HazelCastClusterFileState hcState = getStateFromNearCache(fstate.getPath());
                if (hcState != null) {

                    // Update the file open count

                    hcState.incrementOpenCount();

                    // DEBUG

                    if (hasDebugLevel(DebugNearCache)) {
                        Debug.println("Update near-cache open count state=" + hcState);
                    }
                }
            }

            // Clear any state update post-processor that may be queued

            clearLowPriorityStateUpdates(DisableAllStateUpdates);
        } catch (final ExistingOpLockException ex) {

            // Should not get this error as the remote task verified and granted the oplock

            // DEBUG

            if (hasDebugLevel(DebugFileAccess)) {
                Debug.println("Error saving oplock, fstate=" + fstate + ", params=" + params);
                Debug.println(ex);
            }
        } catch (final ExecutionException ex) {

            // DEBUG

            if (hasDebugLevel(DebugFileAccess)) {
                Debug.println("Error granting access, fstate=" + fstate + ", params=" + params);
                Debug.println(ex);
            }

            // Problem executing the remote task

            if (ex.getCause() != null) {
                if (ex.getCause() instanceof FileSharingException) {
                    throw (FileSharingException) ex.getCause();
                } else if (ex.getCause() instanceof AccessDeniedException) {
                    throw (AccessDeniedException) ex.getCause();
                }
            } else {
                throw new AccessDeniedException("Failed to execute remote grant access on " + fstate.getPath(), ex);
            }
        } catch (final InterruptedException ex) {

            // DEBUG

            if (hasDebugLevel(DebugFileAccess)) {
                Debug.println("Error granting access, fstate=" + fstate + ", params=" + params);
                Debug.println(ex);
            }

            // Problem executing the remote task

            throw new AccessDeniedException("Failed to execute remote grant access on " + fstate.getPath(), ex);
        }

        // Return the access token

        return accessToken;
    }

    /**
     * Release access to a file
     *
     * @param fstate
     *            FileState
     * @param token
     *            FileAccessToken
     * @return int
     */
    @Override
    public int releaseFileAccess(final FileState fstate, final FileAccessToken token) {

        // If there is no token then the file/folder was not granted access, do not update the file state

        if (token == null) {
            return fstate.getOpenCount();
        }

        // Make sure the token is from the cluster

        if (token instanceof HazelCastAccessToken == false) {
            throw new RuntimeException(
                    "Attempt to release Invalid access token type=" + token.getClass().getCanonicalName() + ", file state " + fstate.getPath());
        }

        // DEBUG

        if (hasDebugLevel(DebugFileAccess)) {
            Debug.println("Release file access for state=" + fstate + ", token=" + token);
        }

        // Remove the near cached details

        if (hasNearCache()) {
            m_nearCache.remove(fstate.getPath());
        }

        // Run the file access checks via the node that owns the file state

        final ExecutorService execService = m_hazelCastInstance.getExecutorService();
        final Callable<Integer> callable = new ReleaseFileAccessTask(getClusterName(), fstate.getPath(), token, m_topicName, hasDebugLevel(DebugFileAccess),
                hasTaskTiming());
        final FutureTask<Integer> releaseAccessTask = new DistributedTask<Integer>(callable, fstate.getPath());

        execService.execute(releaseAccessTask);

        int openCnt = -1;

        try {

            // Wait for the remote task to complete, get the updated file open count

            openCnt = releaseAccessTask.get();

            // Clear the local oplock if the token indicates an oplock on the file

            final HazelCastAccessToken hcToken = (HazelCastAccessToken) token;
            hcToken.setReleased(true);

            final PerNodeState perNode = m_perNodeCache.get(fstate.getPath());

            if (perNode != null && perNode.hasOpLock()) {

                // Check if the file token indicates an oplock was granted, or the file open count is now zero

                if (openCnt == 0 || hcToken.getOpLockType() != OpLock.TypeNone) {

                    // Check if the oplock has a break in progress, the client may be closing the file to release the oplock
                    // rather than acknowledging the oplock break

                    if (perNode.getOpLock().hasBreakInProgress()) {

                        // Inform cluster nodes that an oplock has been released

                        final OpLockMessage oplockMsg = new OpLockMessage(ClusterMessage.AllNodes, ClusterMessageType.OpLockBreakNotify, fstate.getPath());
                        m_clusterTopic.publish(oplockMsg);

                        // DEBUG

                        if (hasDebugLevel(DebugFileAccess | DebugOplock)) {
                            Debug.println("Sent oplock break notify for in-progress break, file closed to release oplock, state=" + fstate);
                        }
                    }

                    // Clear the local oplock

                    perNode.clearOpLock();

                    // DEBUG

                    if (hasDebugLevel(DebugFileAccess | DebugOplock)) {
                        Debug.println("Cleared local oplock during token release, token=" + token);
                    }
                }
            }

            // Update the near-cache

            if (hasNearCache()) {

                // Check if the file state is in the near-cache

                final HazelCastClusterFileState hcState = getStateFromNearCache(fstate.getPath());
                if (hcState != null) {

                    // Set the open count

                    hcState.setOpenCount(openCnt);

                    // DEBUG

                    if (hasDebugLevel(DebugNearCache)) {
                        Debug.println("Update near-cache open count state=" + hcState);
                    }

                    // Check if the token indicates an oplock was granted, or the file count is zero

                    if (openCnt == 0 || hcToken.getOpLockType() != OpLock.TypeNone) {

                        // Clear the oplock details

                        hcState.clearOpLock();

                        // DEBUG

                        if (hasDebugLevel(DebugNearCache)) {
                            Debug.println("Cleared oplock from near-cache (release token) state=" + hcState);
                        }
                    }
                }
            }
        } catch (final Exception ex) {

            // DEBUG

            if (hasDebugLevel(DebugFileAccess)) {
                Debug.println("Error releasing access, fstate=" + fstate + ", token=" + token);
                Debug.println(ex);
            }
        }

        // Return the updated open file count

        return openCnt;
    }

    /**
     * Check if the file is readable for the specified section of the file and process id
     *
     * @param clState
     *            ClusterFileState
     * @param offset
     *            long
     * @param len
     *            long
     * @param pid
     *            int
     * @return boolean
     */
    @Override
    public boolean canReadFile(final ClusterFileState clState, final long offset, final long len, final int pid) {

        // Check if the file is open by multiple users

        boolean canRead = true;

        if (clState.getOpenCount() > 1) {

            // Need to check if the file is readable using a remote call, to synchronize the check

            canRead = checkFileAccess(clState, offset, len, pid, false);
        } else if (hasDebugLevel(DebugByteLock)) {
            Debug.println("Check file readable for state=" + clState + ", fileCount=" + clState.getOpenCount());
        }

        // Return the read status

        return canRead;
    }

    /**
     * Check if the file is writeable for the specified section of the file and process id
     *
     * @param clState
     *            ClusterFileState
     * @param offset
     *            long
     * @param len
     *            long
     * @param pid
     *            int
     * @return boolean
     */
    @Override
    public boolean canWriteFile(final ClusterFileState clState, final long offset, final long len, final int pid) {

        // Check if the file is open by multiple users

        boolean canWrite = true;

        if (clState.getOpenCount() > 1) {

            // Need to check if the file is writeable using a remote call, to synchronize the check

            canWrite = checkFileAccess(clState, offset, len, pid, true);
        } else if (hasDebugLevel(DebugByteLock)) {
            Debug.println("Check file writeable for state=" + clState + ", fileCount=" + clState.getOpenCount());
        }

        // Return the write status

        return canWrite;
    }

    /**
     * Check file access using a remote call
     *
     * @param clState
     *            ClusterFileState
     * @param offset
     *            long
     * @param len
     *            long
     * @param pid
     *            int
     * @param writeCheck
     *            boolean
     * @return boolean
     */
    protected boolean checkFileAccess(final ClusterFileState clState, final long offset, final long len, final int pid, final boolean writeCheck) {

        // Create a lock to hold the details of the area to be checked

        final ClusterFileLock checkLock = new ClusterFileLock(getLocalNode(), offset, len, pid);

        // DEBUG

        if (hasDebugLevel(DebugByteLock)) {
            Debug.println("Check file " + (writeCheck ? "writeable" : "readable") + " for state=" + clState + ", area=" + checkLock);
        }

        // Check the file access via a remote call to the node that owns the file state

        final ExecutorService execService = m_hazelCastInstance.getExecutorService();
        final Callable<Boolean> callable = new CheckFileByteLockTask(getClusterName(), clState.getPath(), checkLock, writeCheck, hasDebugLevel(DebugFileAccess),
                hasTaskTiming());
        final FutureTask<Boolean> checkLockTask = new DistributedTask<Boolean>(callable, clState.getPath());

        execService.execute(checkLockTask);
        boolean canAccess = false;

        try {

            // Wait for the remote task to complete

            canAccess = checkLockTask.get().booleanValue();
        } catch (final Exception ex) {

            // DEBUG

            if (hasDebugLevel(DebugByteLock)) {
                Debug.println("Error checking file access, fstate=" + clState + ", area=" + checkLock);
                Debug.println(ex);
            }
        }

        // Return the access status

        return canAccess;
    }

    /**
     * Update a file state using a remote task call
     *
     * @param clState
     *            ClusterFileState
     * @param updateMask
     *            int
     * @return boolean
     */
    protected boolean remoteUpdateState(final ClusterFileState clState, final int updateMask) {

        // DEBUG

        if (hasDebugLevel(DebugRemoteTask | DebugFileStatus)) {
            Debug.println("Remote state update state=" + clState + ", updateMask=" + ClusterFileState.getUpdateMaskAsString(updateMask));
        }

        // Only support file status update for now

        if (updateMask != ClusterFileState.UpdateFileStatus) {
            throw new RuntimeException("Remote state update for " + ClusterFileState.getUpdateMaskAsString(updateMask) + " not supported");
        }

        // Update the file status via a remote call to the node that owns the file state

        final ExecutorService execService = m_hazelCastInstance.getExecutorService();
        final Callable<Boolean> callable = new UpdateStateTask(getClusterName(), clState.getPath(), clState.getFileStatus(),
                hasDebugLevel(DebugRemoteTask | DebugFileStatus), hasTaskTiming());
        final FutureTask<Boolean> updateStateTask = new DistributedTask<Boolean>(callable, clState.getPath());

        execService.execute(updateStateTask);
        boolean stateUpdated = false;

        try {

            // Wait for the remote task to complete

            stateUpdated = updateStateTask.get().booleanValue();

            // If the state was updated then inform cluster members of the change

            if (stateUpdated == true) {

                // Inform cluster members of the state update

                updateFileState(clState, updateMask);

                // Update the near-cache

                if (hasNearCache()) {

                    // Get the local cached value

                    final HazelCastClusterFileState hcState = getStateFromNearCache(clState.getPath());
                    if (hcState != null) {

                        // Update the file status

                        hcState.setFileStatusInternal(clState.getFileStatus(), clState.getStatusChangeReason());

                        // If the status indicates the file/folder no longer exists then clear the file id, state attributes

                        if (clState.getFileStatus() == FileStatus.NotExist) {

                            // Reset the file id

                            hcState.setFileId(FileState.UnknownFileId);

                            // Clear out any state attributes

                            hcState.removeAllAttributes();
                        }

                        // DEBUG

                        if (hasDebugLevel(DebugNearCache)) {
                            Debug.println("Updated near-cache file status, state=" + hcState);
                        }
                    }
                }
            }
        } catch (final Exception ex) {

            // DEBUG

            if (hasDebugLevel(DebugRemoteTask | DebugFileStatus)) {
                Debug.println("Error updating status, fstate=" + clState + ", updateMask=" + ClusterFileState.getUpdateMaskAsString(updateMask));
                Debug.println(ex);
            }
        }

        // Return the update status

        return stateUpdated;
    }

    /**
     * Update a file state, notify the cluster of the updates
     *
     * @param clState
     *            ClusterFileState
     * @param updateMask
     *            int
     */
    @Override
    public void updateFileState(final ClusterFileState clState, final int updateMask) {

        // Create a file status update message and broadcast to the cluster

        final StateUpdateMessage stateUpdMsg = new StateUpdateMessage(ClusterMessage.AllNodes, m_localNode, clState, updateMask);
        m_clusterTopic.publish(stateUpdMsg);

        // DEBUG

        if (hasDebugLevel(DebugClusterMessage)) {
            Debug.println("Sent file state update to cluster, state=" + clState + ", update=" + ClusterFileState.getUpdateMaskAsString(updateMask));
        }
    }

    /**
     * Return the cluster name
     *
     * @return String
     */
    @Override
    public String getClusterName() {
        return m_clusterName;
    }

    /**
     * Return the list of nodes
     *
     * @return ClusterNodeList
     */
    @Override
    public ClusterNodeList getNodeList() {
        return m_nodes;
    }

    /**
     * Return the local node details
     *
     * @return ClusterNode
     */
    @Override
    public ClusterNode getLocalNode() {
        return m_localNode;
    }

    /**
     * Return the associated cluster state cache
     *
     * @return ClusterFileStateCache
     */
    @Override
    public ClusterFileStateCache getStateCache() {
        return this;
    }

    /**
     * Return the thread pool
     *
     * @return ThreadRequestPool
     */
    @Override
    public ThreadRequestPool getThreadPool() {
        return m_threadPool;
    }

    /**
     * Check if none existent file/folder states should be sent to the cluster
     *
     * @return boolean
     */
    @Override
    public boolean hasSendNotExistStates() {
        return m_sendNotExist;
    }

    /**
     * Return the oplock manager
     *
     * @return OpLockManager
     */
    @Override
    public OpLockManager getOpLockManager() {
        return m_oplockManager;
    }

    /**
     * Check if the change notification handler is set
     *
     * @return boolean
     */
    public boolean hasNotifyChangeHandler() {
        return m_notifyHandler != null ? true : false;
    }

    /**
     * Return the change notification handler, if configured for the filesystem
     *
     * @return NotifyChangeHandler
     */
    @Override
    public NotifyChangeHandler getNotifyChangeHandler() {
        return m_notifyHandler;
    }

    /**
     * Set the send none existent file/folder states to the cluster
     *
     * @param notExist
     *            boolean
     */
    @Override
    public void setSendNotExistStates(final boolean notExist) {
        m_sendNotExist = notExist;
    }

    /**
     * Set the oplock manager
     *
     * @param oplockMgr
     *            OpLockManager
     */
    @Override
    public void setOpLockManager(final OpLockManager oplockMgr) {
        m_oplockManager = oplockMgr;
    }

    /**
     * Set the change notification handler
     *
     * @param notifyHandler
     *            NotifyChangeHandler
     */
    @Override
    public void setNotifyChangeHandler(final NotifyChangeHandler notifyHandler) {
        m_notifyHandler = notifyHandler;
    }

    /**
     * Set the cluster node list
     *
     * @param nodeList
     *            ClusterNodeList
     */
    @Override
    public void setNodeList(final ClusterNodeList nodeList) {
        m_nodes = nodeList;
    }

    /**
     * Set the local cluster node
     *
     * @param localNode
     *            ClusterNode
     */
    @Override
    public void setLocalNode(final ClusterNode localNode) {
        m_localNode = localNode;
    }

    /**
     * Check if the specified debug level is enabled
     *
     * @param flg
     *            int
     * @return boolean
     */
    public final boolean hasDebugLevel(final int flg) {
        return (m_debugFlags & flg) != 0 ? true : false;
    }

    /**
     * Check if remote task debugging is enabled
     *
     * @return boolean
     */
    public final boolean hasTaskDebug() {
        return hasDebugLevel(DebugRemoteTask);
    }

    /**
     * Check if remote task timing is enabled
     *
     * @return boolean
     */
    public final boolean hasTaskTiming() {
        return hasDebugLevel(DebugRemoteTiming);
    }

    /**
     * Invoked when an entry is added to the clustered cache
     *
     * @param event
     *            entry event
     */
    @Override
    public void entryAdded(final EntryEvent<String, HazelCastClusterFileState> event) {

        // DEBUG

        if (hasDebugLevel(DebugClusterEntry)) {
            Debug.println("EntryAdded: key=" + event.getKey());
        }
    }

    /**
     * Invoked when an entry is removed from the clustered cache
     *
     * @param event
     *            entry event
     */
    @Override
    public void entryRemoved(final EntryEvent<String, HazelCastClusterFileState> event) {

        // DEBUG

        if (hasDebugLevel(DebugClusterEntry)) {
            Debug.println("EntryRemoved: key=" + event.getKey());
        }

        // Check if there is an entry in the local per-node cache

        final PerNodeState perNode = m_perNodeCache.remove(event.getKey());

        // DEBUG

        if (perNode != null && hasDebugLevel(DebugPerNode)) {
            Debug.println("Removed entry " + event.getKey() + " from per-node cache (remote remove), perNode=" + perNode);
        }

        // Check if the near-cache is enabled, remove from the near-cache

        if (hasNearCache()) {

            // Remove the state from the near-cache

            final HazelCastClusterFileState hcState = m_nearCache.remove(event.getKey());

            // DEBUG

            if (hcState != null && hasDebugLevel(DebugNearCache)) {
                Debug.println("Removed entry from near-cache (remote remove), state=" + hcState);
            }
        }
    }

    /**
     * Invoked when an entry is updated in the clustered cache
     *
     * @param event
     *            entry event
     */
    @Override
    public void entryUpdated(final EntryEvent<String, HazelCastClusterFileState> event) {

        // DEBUG

        if (hasDebugLevel(DebugClusterEntry)) {
            Debug.println("EntryUpdated: key=" + event.getKey());
        }

        // If the near cache is enabled then check if we have the entry cached

        if (hasNearCache()) {

            // Check if the entry is in the near cache

            final HazelCastClusterFileState hcState = getStateFromNearCache(event.getKey());
            if (hcState != null) {

                // Update the remote update time for the near cache version of the file state

                hcState.setNearRemoteUpdateTime();

                // DEBUG

                if (hasDebugLevel(DebugNearCache)) {
                    Debug.println("Near-cache remote update time state=" + hcState);
                }
            }
        }
    }

    /**
     * Invoked when an entry is evicted from the clustered cache
     *
     * @param event
     *            entry event
     */
    @Override
    public void entryEvicted(final EntryEvent<String, HazelCastClusterFileState> event) {

        // DEBUG

        if (hasDebugLevel(DebugClusterEntry)) {
            Debug.println("EntryEvicted: key=" + event.getKey());
        }

        // Check if the near-cache is enabled, remove from the near-cache

        if (hasNearCache()) {

            // Remove the state from the near-cache

            final HazelCastClusterFileState hcState = m_nearCache.remove(event.getKey());

            // DEBUG

            if (hcState != null && hasDebugLevel(DebugNearCache)) {
                Debug.println("Removed entry " + event.getKey() + " from near-cache (remote evict), state=" + hcState);
            }
        }
    }

    /**
     * Cluster topic message listener
     *
     * @param hzMessage
     *            ClusterMessage
     */
    @Override
    public void onMessage(final Message<ClusterMessage> hzMessage) {

        // Check is the message is addressed to this node, or all nodes
        final ClusterMessage msg = hzMessage.getMessageObject();
        if (msg.isAllNodes() || m_localNode.nameMatches(msg.getTargetNode())) {

            // Process the message

            switch (msg.isType()) {

                // Oplock break request

                case ClusterMessageType.OpLockBreakRequest:
                    procOpLockBreakRequest((OpLockMessage) msg);
                    break;

                // Oplock break notify

                case ClusterMessageType.OpLockBreakNotify:
                    procOpLockBreakNotify((OpLockMessage) msg);
                    break;

                // Oplock type changed

                case ClusterMessageType.OplockTypeChange:
                    procOpLockTypeChange((OpLockMessage) msg);
                    break;

                // File state update

                case ClusterMessageType.FileStateUpdate:
                    procFileStateUpdate((StateUpdateMessage) msg);
                    break;

                // File state rename

                case ClusterMessageType.RenameState:
                    procFileStateRename((StateRenameMessage) msg);
                    break;

                // File data update in progress/completed

                case ClusterMessageType.DataUpdate:
                    procDataUpdate((DataUpdateMessage) msg);
                    break;

                // Unknown message type

                default:

                    // DEBUG

                    if (hasDebugLevel(DebugClusterMessage)) {
                        Debug.println("Unknown cluster message msg=" + msg);
                    }
                    break;
            }
        }
    }

    /**
     * Process a remote oplock break request message
     *
     * @param msg
     *            OpLockMessage
     */
    protected void procOpLockBreakRequest(final OpLockMessage msg) {

        // DEBUG

        if (hasDebugLevel(DebugClusterMessage | DebugOplock)) {
            Debug.println("Process oplock break request msg=" + msg);
        }

        // Check if the oplock is owned by the local node

        final PerNodeState perNode = m_perNodeCache.get(msg.getPath());

        if (perNode != null && perNode.hasOpLock()) {

            // Get the local oplock

            final LocalOpLockDetails localOpLock = perNode.getOpLock();

            // DEBUG

            if (hasDebugLevel(DebugClusterMessage | DebugOplock)) {
                Debug.println("Request oplock break, path=" + msg.getPath() + ", via local oplock=" + localOpLock);
            }

            try {

                // Request an oplock break

                localOpLock.requestOpLockBreak();
            } catch (final Exception ex) {

                // DEBUG

                if (hasDebugLevel(DebugClusterMessage | DebugOplock)) {
                    Debug.println("Oplock break failed, ex=" + ex);
                }
            }
        } else if (hasDebugLevel(DebugClusterMessage | DebugOplock)) {

            // Send back an oplock break response to the requestor, oplock already released

            final OpLockMessage oplockMsg = new OpLockMessage(msg.getFromNode(), ClusterMessageType.OpLockBreakNotify, msg.getPath());
            m_clusterTopic.publish(oplockMsg);

            // DEBUG

            Debug.println("No oplock on path=" + msg.getPath());
        }
    }

    /**
     * Process a remote oplock break notify message
     *
     * @param msg
     *            OpLockMessage
     */
    protected void procOpLockBreakNotify(final OpLockMessage msg) {

        // DEBUG

        if (hasDebugLevel(DebugClusterMessage | DebugOplock)) {
            Debug.println("Process oplock break notify msg=" + msg);
        }

        // Check if the path has a state in the near cache, invalidate it

        if (hasNearCache()) {

            // Check if we have the state cached in the near-cache

            final HazelCastClusterFileState hcState = getStateFromNearCache(msg.getPath());
            if (hcState != null) {

                // Invalidate the near-cache entry

                hcState.setStateValid(false);
            }
        }

        // Check if the path has a pending oplock break

        final PerNodeState perNode = m_perNodeCache.get(msg.getPath());

        if (perNode != null && perNode.hasDeferredSessions()) {

            // Cancel the oplock timer for this oplock

            m_oplockManager.cancelOplockTimer(msg.getPath());

            // Requeue the deferred request(s) to the thread pool, oplock released

            perNode.requeueDeferredRequests();
        }
    }

    /**
     * Process a remote oplock type change message
     *
     * @param msg
     *            OpLockMessage
     */
    protected void procOpLockTypeChange(final OpLockMessage msg) {

        // DEBUG

        if (hasDebugLevel(DebugClusterMessage | DebugOplock)) {
            Debug.println("Process oplock change type msg=" + msg);
        }

        // Check if the update came from the local node

        if (msg.isFromLocalNode(m_localNode) == false) {

            // Check if the path has a state in the near cache, invalidate it

            if (hasNearCache()) {

                // Check if we have the state cached in the near-cache

                final HazelCastClusterFileState hcState = getStateFromNearCache(msg.getPath());
                if (hcState != null) {

                    // Invalidate the near-cache entry

                    hcState.setStateValid(false);
                }
            }

            // Check if there are any local sessions waiting on an oplock break/type change

            final PerNodeState perNode = m_perNodeCache.get(msg.getPath());

            if (perNode != null && perNode.hasDeferredSessions()) {

                // Cancel the oplock timer for this oplock

                m_oplockManager.cancelOplockTimer(msg.getPath());

                // Requeue the deferred request(s) to the thread pool, oplock released

                perNode.requeueDeferredRequests();
            }

        }
    }

    /**
     * Process a remote file state update message
     *
     * @param msg
     *            StateUpdateMessage
     */
    protected void procFileStateUpdate(final StateUpdateMessage msg) {

        // DEBUG

        if (hasDebugLevel(DebugClusterMessage)) {
            Debug.println("Process file state update msg=" + msg);
        }

        // Check if this node owns the file state key
        //
        // Note: File status updates are done via a remote task

        HazelCastClusterFileState clState = null;

        if (isLocalKey(msg.getPath()) && msg.getUpdateMask() != ClusterFileState.UpdateFileStatus) {

            // Update the file status in the cache, need to lock/get/put/unlock

            m_stateCache.lock(msg.getPath());

            clState = m_stateCache.get(msg.getPath());

            if (clState != null) {

                // Update the file state

                clState.updateState(msg);

                // Put the updated file state back into the cluster cache

                m_stateCache.put(msg.getPath(), clState);

                // DEBUG

                if (hasDebugLevel(DebugClusterMessage)) {
                    Debug.println("Updated file status, state=" + clState);
                }
            }

            // Unlock the key

            m_stateCache.unlock(msg.getPath());
        }

        // Check if the update came from the local node

        if (msg.isFromLocalNode(m_localNode) == false) {

            // Update the near-cache

            final int reason = msg.getStatusChangeReason();

            if (hasNearCache()) {

                // Check if we have the state cached in the near-cache

                final HazelCastClusterFileState hcState = getStateFromNearCache(msg.getPath());
                if (hcState != null) {

                    // Update the file state

                    hcState.updateState(msg);

                    // Change a NotExist file status to Unknown, so the local node will reinitialize any per node details if required

                    if (msg.hasUpdate(ClusterFileState.UpdateFileStatus) && hcState.getFileStatus() == FileStatus.NotExist) {
                        hcState.setFileStatusInternal(FileStatus.Unknown, FileState.ReasonNone);
                    }

                    // If a file has been deleted or a new version created then clear the file id and cached details
                    // so they are reloaded from the database

                    if (reason == FileState.ReasonFileDeleted || reason == FileState.ReasonFileCreated) {

                        // Clear the file id and cached file details

                        hcState.setFileId(FileState.UnknownFileId);
                        hcState.removeAllAttributes();

                        // DEBUG

                        if (hasDebugLevel(DebugNearCache)) {
                            Debug.println("File " + (reason == FileState.ReasonFileCreated ? " Created" : "Deleted") + ", path=" + msg.getPath()
                                    + ", cleared file id/attributes");
                        }
                    }

                    // DEBUG

                    if (hasDebugLevel(DebugNearCache)) {
                        Debug.println("Updated near-cache file state=" + hcState);
                    }
                }
            }

            // Check if there is cached data in the per-node cache

            final PerNodeState perNode = m_perNodeCache.get(msg.getPath());

            if (perNode != null) {

                // Check if a file has been deleted or a new version created, clear per node cached details

                if (reason == FileState.ReasonFileDeleted || reason == FileState.ReasonFileCreated) {

                    perNode.setFileId(FileState.UnknownFileId);
                    perNode.remoteAllAttributes();

                    // DEBUG

                    if (hasDebugLevel(DebugPerNode)) {
                        Debug.println("Reset fileId, removed attributes for path=" + msg.getPath() + ", perNode=" + perNode + ", reason="
                                + FileState.getChangeReasonString(reason));
                    }
                }
            }

            // Send out change notifications

            if (hasNotifyChangeHandler()) {

                // Check for a file status update

                if (msg.hasUpdate(ClusterFileState.UpdateFileStatus) && msg.getStatusChangeReason() != FileState.ReasonNone) {

                    // Get the file status reason

                    final int reasonCode = msg.getStatusChangeReason();
                    final String path = msg.getPath();

                    switch (reasonCode) {
                        case FileState.ReasonFileCreated:
                            getNotifyChangeHandler().notifyFileChanged(NotifyChange.ActionAdded, path);
                            break;
                        case FileState.ReasonFolderCreated:
                            getNotifyChangeHandler().notifyDirectoryChanged(NotifyChange.ActionAdded, path);
                            break;
                        case FileState.ReasonFileDeleted:
                            getNotifyChangeHandler().notifyFileChanged(NotifyChange.ActionRemoved, path);
                            break;
                        case FileState.ReasonFolderDeleted:
                            getNotifyChangeHandler().notifyDirectoryChanged(NotifyChange.ActionRemoved, path);
                            break;
                    }

                    // DEBUG

                    if (hasDebugLevel(DebugClusterMessage)) {
                        Debug.println("Sent change notification path=" + path + ", reason=" + FileState.getChangeReasonString(reasonCode));
                    }
                }
            }
        }
    }

    /**
     * Process a remote file state rename message
     *
     * @param msg
     *            StateRenameMessage
     */
    protected void procFileStateRename(final StateRenameMessage msg) {

        // DEBUG

        if (hasDebugLevel(DebugClusterMessage)) {
            Debug.println("Process file state rename msg=" + msg);
        }

        // Check if the message is from another node

        if (msg.isFromLocalNode(m_localNode) == false) {

            // Update the per node data to the new path

            final PerNodeState perNode = m_perNodeCache.remove(msg.getOldPath());
            if (perNode != null) {
                m_perNodeCache.put(msg.getNewPath(), perNode);
            }

            // Check if there is a near-cache entry

            if (hasNearCache()) {

                // Check if the file state is in the near-cache

                final HazelCastClusterFileState hcState = m_nearCache.remove(msg.getOldPath());
                if (hcState != null) {

                    // Update the state path

                    hcState.setPath(msg.getNewPath(), isCaseSensitive());

                    // Remove any attributes from the near-cache copy of the state

                    hcState.removeAllAttributes();

                    // Add the entry back using the new path

                    m_nearCache.put(hcState.getPath(), hcState);

                    // DEBUG

                    if (hasDebugLevel(DebugNearCache)) {
                        Debug.println("Rename near-cache entry (remote), from=" + msg.getOldPath() + ", to=" + hcState);
                    }
                }
            }

            // Send out a change notification

            if (hasNotifyChangeHandler()) {

                // Inform local CIFS clients of the rename

                getNotifyChangeHandler().notifyRename(msg.getOldPath(), msg.getNewPath());

                // DEBUG

                if (hasDebugLevel(DebugClusterMessage)) {
                    Debug.println("Sent rename change notification newPath=" + msg.getNewPath());
                }
            }
        }

        // Check if the rename is for a folder, we need to update all locally owned states that are
        // using that path in the main cache, per node cache and near-cache

        if (msg.isFolderPath()) {

            // Get the old and new paths, make sure they are terminated correctly, and normalized

            String oldPathPrefix = msg.getOldPath();
            if (oldPathPrefix.endsWith(FileName.DOS_SEPERATOR_STR) == false) {
                oldPathPrefix = oldPathPrefix + FileName.DOS_SEPERATOR_STR;
            }
            oldPathPrefix = FileState.normalizePath(oldPathPrefix, isCaseSensitive());

            String newPathPrefix = msg.getNewPath();
            if (newPathPrefix.endsWith(FileName.DOS_SEPERATOR_STR) == false) {
                newPathPrefix = newPathPrefix + FileName.DOS_SEPERATOR_STR;
            }
            newPathPrefix = FileState.normalizePath(newPathPrefix, isCaseSensitive());

            // Iterate the locally owned keys in the main cache, check if there are any entries that use the old
            // folder path

            final Set<String> localKeys = m_stateCache.localKeySet();

            // Check if there are any items in the cache

            final StringBuilder newStatePath = new StringBuilder(newPathPrefix.length() + 64);
            newStatePath.append(newPathPrefix);

            if (localKeys.size() > 0) {

                // DEBUG

                if (hasDebugLevel(DebugRename)) {
                    Debug.println("Rename folder, checking local cache entries, oldPath=" + oldPathPrefix);
                }

                // Enumerate the file state cache, only enumerate keys owned locally

                final Iterator<String> keysIter = localKeys.iterator();

                while (keysIter.hasNext()) {

                    // Get the current local key, check if it is below the renamed path

                    final String curKey = keysIter.next();
                    if (curKey.startsWith(oldPathPrefix)) {

                        // Build the new path for the file state

                        newStatePath.setLength(newPathPrefix.length());
                        newStatePath.append(curKey.substring(oldPathPrefix.length()));

                        final String newPath = newStatePath.toString();

                        // We need to move the file state to point to the new parent path

                        m_stateCache.lock(curKey);
                        final HazelCastClusterFileState hcState = m_stateCache.remove(curKey);

                        // Update the file state path, and store in the cache using the new path

                        hcState.setPathInternal(newPath);
                        m_stateCache.put(newPath, hcState);

                        m_stateCache.unlock(curKey);

                        // DEBUG

                        if (hasDebugLevel(DebugRename)) {
                            Debug.println("Renamed state path from=" + curKey + " to=" + newPath);
                        }
                    }
                }
            }

            // Update near cache entries

            if (hasNearCache()) {

                // Enumerate the near cache entries

                final Iterator<String> nearIter = m_nearCache.keySet().iterator();

                while (nearIter.hasNext()) {

                    // Get the current key, check if it is below the renamed path

                    final String nearKey = nearIter.next();

                    if (nearKey.startsWith(oldPathPrefix)) {

                        // Build the new path for the file state

                        newStatePath.setLength(newPathPrefix.length());
                        newStatePath.append(nearKey.substring(oldPathPrefix.length()));

                        final String newPath = newStatePath.toString();

                        // Update the file state path, and store in the cache using the new path

                        final HazelCastClusterFileState hcState = m_nearCache.remove(nearKey);

                        hcState.setPathInternal(newPath);
                        m_nearCache.put(newPath, hcState);

                        // DEBUG

                        if (hasDebugLevel(DebugNearCache | DebugRename)) {
                            Debug.println("Renamed near-cache state from=" + nearKey + " to=" + newPath);
                        }
                    }
                }
            }
        }
    }

    /**
     * Process a remote file data update message
     *
     * @param msg
     *            DataUpdateMessage
     */
    protected void procDataUpdate(final DataUpdateMessage msg) {

        // DEBUG

        if (hasDebugLevel(DebugClusterMessage)) {
            Debug.println("Process file data update msg=" + msg);
        }

        // Check if the message is from another node

        if (msg.isFromLocalNode(m_localNode) == false) {

            // Check if there is a near-cache entry

            if (hasNearCache()) {

                // Check if the file state is in the near-cache

                final HazelCastClusterFileState hcState = m_nearCache.remove(msg.getPath());
                if (hcState != null) {

                    // Update the state, check for start or completion of data update

                    if (msg.isStartOfUpdate()) {

                        // Store the details of the node that is updating the file data

                        hcState.setDataUpdateNode(msg.getFromNode());
                    } else {

                        // Clear the data update status, update completed

                        hcState.setDataUpdateNode(null);
                    }

                    // DEBUG

                    if (hasDebugLevel(DebugNearCache)) {
                        Debug.println("Data update on node=" + msg.getFromNode() + ", to=" + hcState + (msg.isStartOfUpdate() ? ", Start" : ", Completed"));
                    }
                }
            }
        }
    }

    /**
     * Check if the path is in the locally owned cache partition
     *
     * @param path
     *            String
     * @return boolean
     */
    protected boolean isLocalKey(final String path) {

        // Check if the local node owns the partition that the path/key belongs to

        final Partition keyPart = m_hazelCastInstance.getPartitionService().getPartition(path);
        if (keyPart.getOwner().equals(m_localNode.getAddress())) {
            return true;
        }
        return false;
    }

    /**
     * Clear some, or all, low priority state updates that may be queued
     *
     * @param updateMask
     *            int
     */
    protected final void clearLowPriorityStateUpdates(final int updateMask) {

        // Check if there is a state update post processor queued for this thread

        final StateUpdatePostProcessor updatePostProc = (StateUpdatePostProcessor) RequestPostProcessor.findPostProcessor(StateUpdatePostProcessor.class);
        if (updatePostProc != null) {

            // Check if the state update post processor should be removed

            if (updateMask == DisableAllStateUpdates || updatePostProc.getUpdateMask() == updateMask) {

                // Remove the post processor

                RequestPostProcessor.removePostProcessorFromQueue(updatePostProc);

                // DEBUG

                if (hasDebugLevel(DebugClusterMessage)) {
                    Debug.println("Removed state update post processor");
                }
            } else {

                // Remove specific state updates from being sent out at the end of request
                // processing

                updatePostProc.removeFromUpdateMask(updateMask);

                // DEBUG

                if (hasDebugLevel(DebugClusterMessage)) {
                    Debug.println("Removed state updates from post processor, mask=" + ClusterFileState.getUpdateMaskAsString(updateMask));
                }
            }
        }
    }

    /**
     * Update a near-cache state with a new state received from a remote task call
     *
     * @param clState
     *            ClusterFileState
     */
    protected final void updateNearCacheState(final ClusterFileState clState) {

        // Update the locally cached copy of the file state

        if (hasNearCache() && clState instanceof HazelCastClusterFileState) {

            // Check if the state is cached in the near-cache

            final HazelCastClusterFileState curState = getStateFromNearCache(clState.getPath());
            final HazelCastClusterFileState newState = (HazelCastClusterFileState) clState;

            // Copy near-cache details from the current state to the new state

            if (curState != null) {

                // Copy the current near-cache timeout/stats

                newState.copyNearCacheDetails(curState);
            } else {

                // Initialize the near-cache timeout

                newState.setNearCacheTime();
            }

            // Update the near-cache copy with the updated state

            m_nearCache.put(clState.getPath(), newState);

            // DEBUG

            if (hasDebugLevel(DebugNearCache)) {
                Debug.println("Updated near-cache from task result, state=" + newState + (curState != null ? " Copied" : "New"));
            }
        }
    }

    /**
     * Indicate a data update is in progress for the specified file
     *
     * @param fstate
     *            FileState
     */
    @Override
    public void setDataUpdateInProgress(final FileState fstate) {

        // Indicate updated file data exists, update pending/running

        updateFileDataStatus((ClusterFileState) fstate, true);
    }

    /**
     * Indicate that a data update has completed for the specified file
     *
     * @param fstate
     *            FileState
     */
    @Override
    public void setDataUpdateCompleted(final FileState fstate) {

        // Indicate updated file data exists, update completed

        updateFileDataStatus((ClusterFileState) fstate, false);
    }

    /**
     * Update the file data update in progress status for a file state
     *
     * @param fState
     *            ClusterFileState
     * @param startUpdate
     *            boolean
     */
    private void updateFileDataStatus(final ClusterFileState fState, final boolean startUpdate) {

        // DEBUG

        if (hasDebugLevel(DebugFileDataUpdate)) {
            Debug.println("File data update " + (startUpdate ? "started" : "completed") + " on state=" + fState);
        }

        // Set the file data update status via a remote call to the node that owns the file state

        final ExecutorService execService = m_hazelCastInstance.getExecutorService();
        final Callable<Boolean> callable = new FileDataUpdateTask(getClusterName(), fState.getPath(), getLocalNode(), startUpdate,
                hasDebugLevel(DebugFileDataUpdate), hasTaskTiming());
        final FutureTask<Boolean> fileDataUpdateTask = new DistributedTask<Boolean>(callable, fState.getPath());

        execService.execute(fileDataUpdateTask);

        try {

            // Wait for the remote task to complete

            if (fileDataUpdateTask.get().booleanValue() == true) {

                // Update the locally cached copy of the file state

                if (hasNearCache() && fState instanceof HazelCastClusterFileState) {

                    // Check if the state is cached in the near-cache

                    final HazelCastClusterFileState nearState = getStateFromNearCache(fState.getPath());
                    if (nearState != null) {

                        // Update the file data update details

                        nearState.setDataUpdateNode(startUpdate ? getLocalNode() : null);

                        // DEBUG

                        if (hasDebugLevel(DebugNearCache)) {
                            Debug.println("Updated near-cache (file data update), state=" + nearState);
                        }
                    }
                }

                // Create a file data update message and broadcast to the cluster

                final DataUpdateMessage dataUpdMsg = new DataUpdateMessage(ClusterMessage.AllNodes, m_localNode, fState.getPath(), startUpdate);
                m_clusterTopic.publish(dataUpdMsg);

                // DEBUG

                if (hasDebugLevel(DebugClusterMessage)) {
                    Debug.println("Sent file data update to cluster, state=" + fState + ", startUpdate=" + startUpdate);
                }
            }
        } catch (final Exception ex) {

            // DEBUG

            if (hasDebugLevel(DebugFileDataUpdate)) {
                Debug.println("Error setting file data update, fstate=" + fState + ", startUpdate=" + startUpdate);
                Debug.println(ex);
            }
        }
    }

    /**
     * Try and get a file state from the near cache
     *
     * @param path
     *            String
     * @return HazelCastClusterFileState
     */
    protected final HazelCastClusterFileState getStateFromNearCache(final String path) {

        // Check if the near-cache is enabled

        HazelCastClusterFileState hcState = null;

        if (hasNearCache()) {

            // See if we have a local copy of the file state, and it is valid

            hcState = m_nearCache.get(path);

            if (hcState != null) {

                // If the locally cached state is valid then update the hit counter

                if (hcState.isStateValid() && hcState.hasExpired(System.currentTimeMillis()) == false) {

                    // Update the cache hit counter

                    hcState.incrementNearCacheHitCount();

                    // DEBUG

                    if (hasDebugLevel(DebugNearCache)) {
                        Debug.println("Found state in near-cache state=" + hcState);
                    }
                } else {

                    // Do not use the near cache state, remove it from the cache

                    hcState = null;
                    m_nearCache.remove(path);

                    // DEBUG

                    if (hasDebugLevel(DebugNearCache)) {
                        Debug.println("Removed invalid state from near-cache state=" + hcState);
                    }
                }
            }
        }

        // Return the file state, or null if not found/not valid

        return hcState;
    }
}
