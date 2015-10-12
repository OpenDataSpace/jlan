package org.alfresco.jlan.test.integration;

import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.io.IOException;
import java.lang.ThreadLocal;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.*;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Optional;
import org.alfresco.jlan.client.CIFSDiskSession;
import org.alfresco.jlan.client.SessionFactory;
import org.alfresco.jlan.client.SessionSettings;
import org.alfresco.jlan.smb.PCShare;
import org.alfresco.jlan.smb.SMBException;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jcifs.smb.SmbFile;
import jcifs.smb.SmbException;
import jcifs.Config;

public class ParameterizedJcifsTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParameterizedJcifsTest.class);

    private static AtomicLong firstThreadId = new AtomicLong();
    private static AtomicInteger virtualCircuitId = new AtomicInteger(0);

    private static String m_host;
    private static String m_user;
    private static String m_pass;
    private static String m_share;
    private static Integer m_cifsport;

    private String testname;
    private String m_path;
    private SmbFile m_root;
    private ThreadLocal<List<String>> filesToDelete = new ThreadLocal<List<String>>() {
        @Override public List<String> initialValue() {
            return new ArrayList<String>();
        }
    };
    private ThreadLocal<List<String>> foldersToDelete = new ThreadLocal<List<String>>() {
        @Override public List<String> initialValue() {
            return new ArrayList<String>();
        }
    };
    protected ParameterizedJcifsTest(final String name) {
        testname = name;
    }

    @Parameters({"host", "user", "pass", "share", "cifsport"})
    @BeforeSuite(alwaysRun = true)
    public static void initSuite(
            final String host,
            final String user,
            final String pass,
            final String share,
            @Optional Integer cifsport)
    {
        m_host = host;
        m_user = user;
        m_pass = pass;
        m_share = share;
        m_cifsport = cifsport;

        Security.addProvider(new BouncyCastleProvider());
    }

    protected SmbFile getRoot() {
        return m_root;
    }

    protected String getTestname() {
        return testname;
    }

    protected boolean isFirstThread() {
        return firstThreadId.get() == Thread.currentThread().getId();
    }

    @BeforeMethod(alwaysRun = true)
    public void BeforeMethod(Method m) throws Exception {
        Thread.currentThread().setName("T" + Thread.currentThread().getId());
        firstThreadId.compareAndSet(0L, Thread.currentThread().getId());
        LOGGER.info("Starting {}.{}", getTestname(), m.getName());
        assertNotNull(m_host, "Target host");
        assertNotNull(m_share, "Target share");
        assertNotNull(m_user, "Target user");
        assertNotNull(m_pass, "Target pass");
        String url = "smb://" + m_user + ":" + m_pass + "@" + m_host;
        if (null != m_cifsport) {
            url += ":" + m_cifsport;
        }
        url += "/" + m_share + "/";
        Config.setProperty("jcifs.resolveOrder", "DNS");
        Config.setProperty("jcifs.smb.client.attrExpirationPeriod", "0");
        Config.setProperty("jcifs.util.loglevel", "10");
        // Config.setProperty("jcifs.smb.client.ssnLimit", "1");
        m_root = new SmbFile(url);
        assertNotNull(getRoot(), "Root");
    }

    @AfterMethod(alwaysRun = true)
    public void afterMethod(final Method m) throws Exception {
        if (!filesToDelete.get().isEmpty()) {
            LOGGER.debug("Cleaning up files of test {}", getTestname());
        }
        // Delete the test files
        for (final String name : filesToDelete.get()) {
            try {
                final SmbFile file = new SmbFile(getRoot(), name);
                if (file.exists()) {
                    file.delete();
                }
            } catch (SmbException e) {
                LOGGER.warn("Cleanup file {} failed: {}", name, e.getMessage());
            }
        }
        filesToDelete.get().clear();
        if (!foldersToDelete.get().isEmpty()) {
            LOGGER.debug("Cleaning up folders of test {}", getTestname());
        }
        // Delete the test folders in reverse order
        Collections.reverse(foldersToDelete.get());
        for (final String name : foldersToDelete.get()) {
            try {
                final SmbFile file = new SmbFile(getRoot(), name);
                if (file.exists()) {
                    file.delete();
                }
            } catch (SmbException e) {
                LOGGER.warn("Cleanup folder {} failed: {}", name, e.getMessage());
            }
        }
        foldersToDelete.get().clear();
        LOGGER.info("Finished {}.{}", getTestname(), m.getName());
    }

    /**
     * Generate a test file name that is unique per test
     *
     * @param iteration int
     * @return String
     */
    public final String getPerTestFileName(int iteration) {
        StringBuilder fileName = new StringBuilder();
        if (getPath() != null) {
            fileName.append(getPath());
        }
        fileName.append(getTestname());
        fileName.append("_");
        fileName.append(iteration);
        fileName.append(".txt");

        filesToDelete.get().add(fileName.toString());
        return fileName.toString();
    }

    /**
     * Generate a test file name that is unique per thread
     *
     * @param iteration int
     * @return String
     */
    public final String getPerThreadFileName(int iteration) {
        StringBuilder fileName = new StringBuilder();
        if (getPath() != null) {
            fileName.append(getPath());
        }
        fileName.append(getTestname());
        fileName.append("_");
        fileName.append(Thread.currentThread().getId());
        fileName.append("_");
        fileName.append(iteration);
        fileName.append(".txt");

        filesToDelete.get().add(fileName.toString());
        return fileName.toString();
    }

    /**
     * Generate a test folder name that is unique per test
     *
     * @param iteration int
     * @return String
     */
    public final String getPerTestFolderName(int iteration) {
        StringBuilder fileName = new StringBuilder();
        if (getPath() != null) {
            fileName.append(getPath());
        }
        fileName.append(getTestname());
        fileName.append("_");
        fileName.append(iteration);
        fileName.append("/");

        foldersToDelete.get().add(fileName.toString());
        return fileName.toString();
    }

    /**
     * Generate a test folder name that is unique per thread
     *
     * @param iteration int
     * @return String
     */
    public final String getPerThreadFolderName(int iteration) {
        StringBuilder fileName = new StringBuilder();
        if (getPath() != null) {
            fileName.append(getPath());
        }
        fileName.append(getTestname());
        fileName.append("_");
        fileName.append(Thread.currentThread().getId());
        fileName.append("_");
        fileName.append(iteration);
        fileName.append("/");

        foldersToDelete.get().add(fileName.toString());
        return fileName.toString();
    }

    /**
     * Generate a unique test file name
     *
     * @param iteration int
     * @return String
     */
    public final String getUniqueFileName(int iteration) {
        StringBuilder fileName = new StringBuilder();
        if (getPath() != null) {
            fileName.append(getPath());
        }
        fileName.append(getTestname());
        fileName.append("_");
        fileName.append(Thread.currentThread().getId());
        fileName.append("_");
        fileName.append(iteration);
        fileName.append("_");
        fileName.append(getRoot().getServer());
        fileName.append(".txt");

        filesToDelete.get().add(fileName.toString());
        return fileName.toString();
    }

    /**
     * Generate a unique test folder name
     *
     * @param iteration int
     * @return String
     */
    public final String getUniqueFolderName(int iteration) {
        StringBuilder fileName = new StringBuilder();
        if (getPath() != null) {
            fileName.append(getPath());
        }
        fileName.append(getTestname());
        fileName.append("_");
        fileName.append(Thread.currentThread().getId());
        fileName.append("_");
        fileName.append(iteration);
        fileName.append("_");
        fileName.append(getRoot().getServer());
        fileName.append("/");

        foldersToDelete.get().add(fileName.toString());
        return fileName.toString();
    }

    public final void registerFileNameForDelete(final String name) {
        filesToDelete.get().add(name);
    }

    public final void registerFolderNameForDelete(final String name) {
        foldersToDelete.get().add(name);
    }

    /**
     * Set the test path
     *
     * @param path String
     */
    public final void setPath(String path) {
        m_path = path;
        if (m_path != null) {
            if (m_path.endsWith("/") == false)
                m_path += "/";
        }
    }

    /**
     * Return the test relative path
     *
     * @return String
     */
    public final String getPath() {
        return m_path;
    }

    /**
     * Sleep for a while
     *
     * @param sleepMs long
     */
    protected final void testSleep(long sleepMs) {
        try {
            Thread.sleep( sleepMs);
        } catch (InterruptedException ex) {
        }
    }

    public static final CIFSDiskSession createCifsSess(String host, String sharename, String username, String password, Integer cifsport) throws UnknownHostException, IOException, SMBException {
        SessionFactory.setSMBSigningEnabled(false);
        PCShare share = new PCShare(host, sharename, username, password);
        SessionSettings sessSettings = new SessionSettings();
        sessSettings.setNativeSMBPort(cifsport);
        sessSettings.setVirtualCircuit(virtualCircuitId.incrementAndGet());
        return (CIFSDiskSession) SessionFactory.OpenDisk(share, sessSettings);
    };

    protected final CIFSDiskSession createCifsSess() throws UnknownHostException, IOException, SMBException {
        return createCifsSess(m_host, m_share, m_user, m_pass, m_cifsport);
    };
}
