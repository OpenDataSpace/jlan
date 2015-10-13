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

package org.alfresco.jlan.oncrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ONC/RPC Request Thread Pool Class
 *
 * <p>
 * Processes RPC requests using a pool of worker threads.
 *
 * @author gkspencer
 */
public class RpcRequestThreadPool {
    private static final Logger LOGGER = LoggerFactory.getLogger(RpcRequestThreadPool.class);
    // Default/minimum/maximum number of worker threads to use
    public static final int DefaultWorkerThreads = 8;
    public static final int MinimumWorkerThreads = 4;
    public static final int MaximumWorkerThreads = 50;

    // Queue of RPC requests
    private final RpcRequestQueue m_queue;

    // Worker threads
    private final ThreadWorker[] m_workers;

    // RPC dispatcher
    private final RpcProcessor m_rpcProcessor;

    // Debug enable flag
    private static boolean m_debug = true;

    /**
     * Thread Worker Inner Class
     */
    protected class ThreadWorker implements Runnable {
        // Worker thread
        private final Thread mi_thread;

        // Shutdown flag
        private boolean mi_shutdown = false;

        /**
         * Class constructor
         *
         * @param name
         *            String
         * @param id
         *            int
         */
        public ThreadWorker(final String name, final int id) {
            // Create the worker thread
            mi_thread = new Thread(this);
            mi_thread.setName(name);
            mi_thread.setDaemon(true);
            mi_thread.start();
        }

        /**
         * Request the worker thread to shutdown
         */
        public final void shutdownRequest() {
            mi_shutdown = true;
            try {
                mi_thread.interrupt();
            } catch (final Exception ex) {
            }
        }

        /**
         * Run the thread
         */
        @Override
        public void run() {
            // Loop until shutdown
            RpcPacket rpc = null;
            RpcPacket response = null;
            while (mi_shutdown == false) {
                try {
                    // Wait for an RPC request to be queued
                    rpc = m_queue.removeRequest();
                } catch (final InterruptedException ex) {
                    // Check for shutdown
                    if (mi_shutdown == true) {
                        break;
                    }
                }

                // If the request is valid process it
                if (rpc != null) {
                    try {
                        // Process the request
                        response = m_rpcProcessor.processRpc(rpc);
                        if (response != null) {
                            response.getPacketHandler().sendRpcResponse(response);
                        }
                    } catch (final Throwable ex) {
                        // Do not display errors if shutting down
                        if (mi_shutdown == false) {
                            LOGGER.error("Worker " + Thread.currentThread().getName() + ":", ex);
                        }
                    } finally {
                        // Release the RPC packet(s) back to the packet pool
                        if (rpc.getClientProtocol() == Rpc.TCP && rpc.isAllocatedFromPool()) {
                            rpc.getOwnerPacketPool().releasePacket(rpc);
                        }

                        if (response != null && response.getClientProtocol() == Rpc.TCP && response.getBuffer() != rpc.getBuffer()
                                && response.isAllocatedFromPool()) {
                            response.getOwnerPacketPool().releasePacket(response);
                        }

                    }
                }
            }
        }
    };

    /**
     * Class constructor
     *
     * @param threadName
     *            String
     * @param rpcServer
     *            RpcProcessor
     */
    public RpcRequestThreadPool(final String threadName, final RpcProcessor rpcServer) {
        this(threadName, DefaultWorkerThreads, rpcServer);
    }

    /**
     * Class constructor
     *
     * @param threadName
     *            String
     * @param poolSize
     *            int
     * @param rpcServer
     *            RpcProcessor
     */
    public RpcRequestThreadPool(final String threadName, int poolSize, final RpcProcessor rpcServer) {
        // Save the RPC handler
        m_rpcProcessor = rpcServer;

        // Create the request queue
        m_queue = new RpcRequestQueue();

        // Check that we have at least minimum worker threads
        if (poolSize < MinimumWorkerThreads) {
            poolSize = MinimumWorkerThreads;
        }

        // Create the worker threads
        m_workers = new ThreadWorker[poolSize];

        for (int i = 0; i < m_workers.length; i++) {
            m_workers[i] = new ThreadWorker(threadName + (i + 1), i);
        }
    }

    /**
     * Check if debug output is enabled
     *
     * @return boolean
     */
    public final static boolean hasDebug() {
        return m_debug;
    }

    /**
     * Return the number of requests in the queue
     *
     * @return int
     */
    public final int getNumberOfRequests() {
        return m_queue.numberOfRequests();
    }

    /**
     * Queue an RPC request to the thread pool for processing
     *
     * @param pkt
     *            RpcPacket
     */
    public final void queueRpcRequest(final RpcPacket pkt) {
        m_queue.addRequest(pkt);
    }

    /**
     * Shutdown the thread pool and release all resources
     */
    public void shutdownThreadPool() {
        // Shutdown the worker threads
        if (m_workers != null) {
            for (final ThreadWorker m_worker : m_workers) {
                m_worker.shutdownRequest();
            }
        }
    }
}
