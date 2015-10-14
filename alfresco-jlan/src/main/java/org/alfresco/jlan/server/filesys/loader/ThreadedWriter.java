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

package org.alfresco.jlan.server.filesys.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Threaded Writer Class
 *
 * <p>
 * Allows a network protocol handler to queue a write request to a thread pool for delayed writing.
 *
 * @author gkspencer
 */
public class ThreadedWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadedWriter.class);

    // Default/minimum/maximum number of worker threads to use
    public static final int DefaultWorkerThreads = 8;
    public static final int MinimumWorkerThreads = 4;
    public static final int MaximumWorkerThreads = 50;

    // Queue of delayed write requests
    private final WriteRequestQueue m_queue;

    // Worker threads
    private final ThreadWorker[] m_workers;

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
            WriteRequest writeReq = null;
            while (mi_shutdown == false) {
                try {
                    // Wait for a write request to be queued
                    writeReq = m_queue.removeRequest();
                } catch (final InterruptedException ex) {
                    // Check for shutdown
                    if (mi_shutdown == true) {
                        break;
                    }
                }

                // If the write request is valid process it
                if (writeReq != null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("ThreadedWriter writeReq={}", writeReq);
                    }

                    // Perform the delayed write request
                    writeReq.doWrite();
                }
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("ThreadedWriter thread={} shutdown", mi_thread.getName());
            }
        }
    };

    /**
     * Class constructor
     */
    public ThreadedWriter() {
        // Create the request queue
        m_queue = new WriteRequestQueue();

        // Create the worker threads
        m_workers = new ThreadWorker[DefaultWorkerThreads];
        for (int i = 0; i < m_workers.length; i++) {
            m_workers[i] = new ThreadWorker("ThreadedWriter_" + (i + 1), i);
        }
    }

    /**
     * Add a delayed write request to the queue
     *
     * @param req
     *            WriteRequest
     */
    public final void addWriteRequest(final WriteRequest req) {
        m_queue.addRequest(req);
    }

    /**
     * Shutdown the threaded writer and release all resources
     */
    public void shutdownWriter() {
        // Shutdown the worker threads
        if (m_workers != null) {
            for (final ThreadWorker m_worker : m_workers) {
                m_worker.shutdownRequest();
            }
        }
    }
}