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

package org.alfresco.jlan.test.cluster;

import java.io.StringWriter;
import java.security.Provider;
import java.security.Security;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.alfresco.jlan.client.DiskSession;
import org.alfresco.jlan.client.SessionFactory;
import org.alfresco.jlan.client.SessionSettings;
import org.alfresco.jlan.smb.PCShare;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster Test Application
 *
 * @author gkspencer
 */
public class ClusterTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTest.class);
    // Constants
    //
    // Barrier wait time
    private static long BarrierWaitTimeout = 60L; // seconds

    // Test configuration
    private final TestConfiguration m_config;

    // Test threads and synchronization object
    private TestThread[] m_testThreads;
    private CyclicBarrier m_startBarrier;
    private CyclicBarrier m_stopBarrier;

    // Test thread sets bit when test completed
    private BitSet m_testDone;

    // Test results
    private TestResult[] m_results;

    // Test result to indicate a test thread did not complete
    private final TestResult m_didNotFinishResult = new BooleanTestResult(false, "Test thread did not complete");

    /**
     * Test Thread Inner Class
     */
    class TestThread extends Thread {
        // Thread name and id
        private final String m_name;
        private final int m_id;

        // Test iteration
        private final int m_iter;

        // Server details
        private final TestServer m_server;

        // Test to run
        private final Test m_test;

        // Thread is waiting on sync object
        private boolean m_wait;

        // Thread has completed test iterations
        private boolean m_complete;

        /**
         * Class constructor
         *
         * @param server
         *            TestServer
         * @param test
         *            Test
         * @param id
         *            int
         * @param iter
         *            int
         */
        public TestThread(final TestServer server, final Test test, final int id, final int iter) {
            m_server = server;
            m_test = test;

            // Set the thread name
            m_name = m_test.getName() + "_" + id;
            m_id = id;

            m_iter = iter;
        }

        /**
         * Run the test
         */
        @Override
        public void run() {
            // Set the thread name
            Thread.currentThread().setName(m_name);
            if (m_test.isVerbose()) {
                LOGGER.debug("{} running, using server {}", m_name, m_server.getName());
            }

            // Indicate test running

            m_complete = false;

            // Connect to the remote server

            PCShare share = null;
            DiskSession sess = null;
            boolean initOK = false;

            try {

                // Connect to the remote server

                share = new PCShare(m_server.getName(), m_server.getShareName(), m_server.getUserName(), m_server.getPassword());

                // Give each session a different virtual circuit id, this allows the test to be run against a Windows file
                // server without the file server closing sessions down

                final SessionSettings sessSettings = new SessionSettings();
                sessSettings.setVirtualCircuit(m_id);

                sess = SessionFactory.OpenDisk(share, sessSettings);

                // Give each thread a unique process id

                sess.setProcessId(m_id);

                // Set the working directory

                if (m_test.getPath() != null) {

                    // Primary thread sets up the test folder

                    if (isPrimaryThread()) {

                        // Check if the remote path exists

                        if (sess.FileExists(m_test.getPath()) == false) {

                            // Create the test folder

                            sess.CreateDirectory(m_test.getPath());
                        }
                    }

                    // Set the working directory

                    sess.setWorkingDirectory(m_test.getPath());
                }

                // Wait for all threads

                waitAtStartBarrier();

                // Initialize the test

                initOK = m_test.initTest(m_id, m_iter, sess);
                if (initOK == false) {
                    LOGGER.warn("Failed to initialize test " + m_test.getName());
                }

                // Set the test result to 'not finished'

                m_results[m_id - 1] = m_didNotFinishResult;

                // Wait for all threads

                waitAtStopBarrier();
            } catch (final Exception ex) {
                LOGGER.error("Error server=" + m_server, ex);
            } finally {

                // Check if initialization was successful

                if (initOK == false) {

                    // Close the session

                    if (sess != null) {
                        try {
                            sess.CloseSession();
                            sess = null;
                        } catch (final Exception ex) {
                        }
                    }
                }
            }

            // Run the test if connected successfully

            if (sess != null) {

                // Loop through the test iterations

                int iteration = 1;

                while (iteration <= m_iter) {

                    // Perform per run setup

                    if (m_test.runInit(m_id, iteration, sess) == false) {
                        LOGGER.warn("Run initialization failed, id=" + m_id + ", iter=" + iteration);
                    }

                    // Create the per test thread output

                    final StringWriter testLog = new StringWriter(512);

                    // Wait on synchronization object

                    waitAtStartBarrier();

                    try {
                        // Start of test setup
                        if (m_id == 1) {
                            LOGGER.info("------- Start iteration " + iteration + " for " + m_test.getName() + " --- " + new Date() + " -----");
                        }

                        // Run the test
                        final TestResult result = m_test.runTest(m_id, iteration, sess, testLog);

                        // Save the test results
                        m_results[m_id - 1] = result;
                    } catch (final Exception ex) {
                        LOGGER.warn(ex.getMessage(), ex);
                    }

                    // Wait for all threads to complete the test
                    waitAtStopBarrier();

                    // Run test cleanup
                    if (m_test.hasTestCleanup()) {
                        // Wait for all threads to reach this point
                        waitAtStartBarrier();

                        try {
                            m_test.cleanupTest(m_id, iteration, sess, testLog);
                        } catch (final Exception ex) {
                            LOGGER.warn(getName() + " Exception during cleanup", ex);
                        }

                        // Wait for all threads to finish cleanup

                        waitAtStopBarrier();
                    }

                    // Dump the test log

                    if (testLog.getBuffer().length() > 0) {
                        LOGGER.info(testLog.toString());
                    }

                    // Check the test results

                    if (m_id == 1) {

                        final List<TestResult> resultsList = Arrays.asList(m_results);
                        final TestResult finalResult = m_test.processTestResults(resultsList);

                        if (finalResult.isSuccess() == false) {
                            LOGGER.info("Final test result: {}", finalResult);
                            LOGGER.info("Test results:");

                            for (final TestResult result : resultsList) {
                                LOGGER.info("" + result);
                            }
                            LOGGER.info("");
                        }

                        LOGGER.info("------- End iteration " + iteration + " for " + m_test.getName() + "   --- " + new Date() + " -----");
                    }

                    // Update the iteration count

                    iteration++;
                }

                // Close the session

                try {
                    sess.CloseSession();
                } catch (final Exception ex) {
                    LOGGER.warn(ex.getMessage(), ex);
                }
            }
            // Indicate that the test is complete

            m_complete = true;
        }

        /**
         * Check if the test thread has completed
         *
         * @return boolean
         */
        public final boolean isComplete() {
            return m_complete;
        }

        /**
         * Check if the thread is waiting on the synchronization object
         *
         * @return boolean
         */
        public boolean isWaiting() {
            return m_wait;
        }

        /**
         * Check if this is the primary thread
         *
         * @return boolean
         */
        public boolean isPrimaryThread() {
            return m_id == 1 ? true : false;
        }

        /**
         * Wait for all test threads
         */
        protected void waitAtStartBarrier() {

            try {
                // Primary thread resets the stop barrier

                if (m_id == 1) {
                    m_stopBarrier.reset();
                }

                m_wait = true;
                m_startBarrier.await(BarrierWaitTimeout, TimeUnit.SECONDS);
            } catch (final Exception ex) {
                ex.printStackTrace();
            }

            // Clear the wait flag

            m_wait = false;
        }

        /**
         * Wait for all test threads
         */
        protected void waitAtStopBarrier() {

            try {
                // Primary thread resets the start barrier

                if (m_id == 1) {
                    m_startBarrier.reset();
                }

                m_wait = true;
                m_stopBarrier.await(BarrierWaitTimeout, TimeUnit.SECONDS);
            } catch (final Exception ex) {
                ex.printStackTrace();
            }

            // Clear the wait flag

            m_wait = false;
        }
    };

    /**
     * Class constructor
     *
     * @param args
     *            String[]
     */
    public ClusterTest(final String[] args) throws Exception {

        // Load the test configuration

        m_config = new TestConfiguration();
        m_config.loadConfiguration(args[0]);
    }

    /**
     * Run the tests
     */
    public void runTests() {

        // Setup the JCE provider, required by the JLAN Client code

        try {
            final Provider provider = (Provider) Class.forName("cryptix.jce.provider.CryptixCrypto").newInstance();
            Security.addProvider(provider);
        } catch (final Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }

        // Global JLAN Client setup

        SessionFactory.setSMBSigningEnabled(false);

        // Startup information

        LOGGER.info("----- Cluster Tests Running --- " + new Date() + " -----");
        LOGGER.info("Run tests: " + (m_config.runInterleaved() ? "Interleaved" : "Sequentially"));
        LOGGER.info("Threads per server: " + m_config.getThreadsPerServer());
        LOGGER.info("");

        LOGGER.info("Servers configured:");
        final StringBuilder serverListBuilder = new StringBuilder();
        for (final TestServer testSrv : m_config.getServerList()) {
            serverListBuilder.append("  ").append(testSrv.getName());
        }
        LOGGER.info(serverListBuilder.toString());
        LOGGER.info("");
        LOGGER.info("");

        LOGGER.info("Tests configured:");

        for (final Test test : m_config.getTestList()) {
            LOGGER.info("  {}", test.toString());
        }

        LOGGER.info("");

        // Calculate the number of test threads

        final int numTestThreads = m_config.getServerList().size() * m_config.getThreadsPerServer();

        // Setup the thread synchronization object

        m_startBarrier = new CyclicBarrier(numTestThreads);
        m_stopBarrier = new CyclicBarrier(numTestThreads);

        // Create the test thread completion bit set

        m_testDone = new BitSet(numTestThreads);

        // Create the per iteration test result list

        m_results = new TestResult[numTestThreads];

        // Loop through the tests

        for (final Test curTest : m_config.getTestList()) {

            // Start of test setup

            LOGGER.info("----- Start test " + curTest.getName() + " --- " + new Date() + " -----");

            // Setup the test threads

            m_testThreads = new TestThread[numTestThreads];

            int idx = 0;

            for (final TestServer curSrv : m_config.getServerList()) {

                // Create test thread(s) for the current test

                for (int perSrv = 0; perSrv < m_config.getThreadsPerServer(); perSrv++) {

                    // Create a test thread

                    final TestThread testThread = new TestThread(curSrv, curTest, idx + 1, curTest.getIterations());
                    m_testThreads[idx] = testThread;
                    idx++;

                    // Start the thread

                    testThread.setDaemon(true);
                    testThread.start();
                }
            }

            // Wait for tests to run

            int waitThread = m_testThreads.length;

            while (waitThread > 0) {

                try {
                    Thread.sleep(100L);
                } catch (final Exception ex) {
                }

                // Check if the test threads have completed

                waitThread = 0;

                for (final TestThread testThread : m_testThreads) {
                    if (testThread.isComplete() == false) {
                        waitThread++;
                    }
                }
            }

            // Clear down the test threads

            for (final TestThread curThread : m_testThreads) {

                // Stop the current thread, if still alive

                if (curThread.isAlive()) {
                    curThread.interrupt();
                }
            }

            m_testThreads = null;

            // Run the garbage collector

            System.gc();

            // End of current test

            LOGGER.info("----- End test " + curTest.getName() + " --- " + new Date() + " -----");
        }

        // End of all tests

        LOGGER.info("-- End all tests --- " + new Date() + " --");
    }

    /**
     * Application startup
     *
     * @param args
     *            String[]
     */
    public static void main(final String[] args) {

        // Check there are enough command line parameters

        if (args.length == 0) {
            System.out.println("Usage: <testConfig XML file>");
            System.exit(1);
        }

        try {

            // Create the cluster tests

            final ClusterTest clusterTest = new ClusterTest(args);
            clusterTest.runTests();
        } catch (final Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }
}
