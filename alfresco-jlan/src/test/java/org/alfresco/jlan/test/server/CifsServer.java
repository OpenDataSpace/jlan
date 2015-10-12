package org.alfresco.jlan.test.server;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.Security;
import java.util.TimeZone;

import org.alfresco.jlan.client.CIFSDiskSession;
import org.alfresco.jlan.server.NetworkServer;
import org.alfresco.jlan.server.ServerListener;
import org.alfresco.jlan.server.auth.EnterpriseCifsAuthenticator;
import org.alfresco.jlan.server.auth.UserAccount;
import org.alfresco.jlan.server.auth.UserAccountList;
import org.alfresco.jlan.server.config.CoreServerConfigSection;
import org.alfresco.jlan.server.config.GlobalConfigSection;
import org.alfresco.jlan.server.config.InvalidConfigurationException;
import org.alfresco.jlan.server.config.SecurityConfigSection;
import org.alfresco.jlan.server.config.ServerConfiguration;
import org.alfresco.jlan.server.core.SharedDevice;
import org.alfresco.jlan.server.filesys.DiskDeviceContext;
import org.alfresco.jlan.server.filesys.DiskSharedDevice;
import org.alfresco.jlan.server.filesys.FilesystemsConfigSection;
import org.alfresco.jlan.smb.Dialect;
import org.alfresco.jlan.smb.DialectSelector;
import org.alfresco.jlan.smb.SMBException;
import org.alfresco.jlan.smb.server.CIFSConfigSection;
import org.alfresco.jlan.smb.server.SMBServer;
import org.alfresco.jlan.smb.server.SMBSrvSession;
import org.alfresco.jlan.smb.server.SecurityMode;
import org.alfresco.jlan.smb.server.disk.EnhJavaFileDiskDriver;
import org.alfresco.jlan.test.integration.ParameterizedJcifsTest;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.springframework.extensions.config.ConfigElement;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class CifsServer {
    @BeforeSuite(alwaysRun = true)
    public static void initSuite(){
        Security.addProvider(new BouncyCastleProvider());
    }

    // Default memory pool settings
    private static final int[] DefaultMemoryPoolBufSizes  = { 256, 4096, 16384, 66000 };
    private static final int[] DefaultMemoryPoolInitAlloc = {  20,   20,     5,     5 };
    private static final int[] DefaultMemoryPoolMaxAlloc  = { 100,   50,    50,    50 };
    // Default thread pool size
    private static final int DefaultThreadPoolInit  = 25;
    private static final int DefaultThreadPoolMax   = 50;
    @Parameters({
        "hostname",
        "workgroup",
        "broadcastMask",
        "comment",
        "announceInterval",
        "sessionPort",
        "cifsPort",
        "namingPort",
        "datagramPort",
        "shareName",
        "localSharePath"})
    @Test
    public void createInstance(
            @Optional("localhost") String hostname,
            @Optional("WORKGROUP") String workgroup,
            @Optional("127.255.255.255") String broadcastMask,
            @Optional("Test CIFS Server") String comment,
            @Optional("5") int announceInterval,
            @Optional("11139") int sessionPort,
            @Optional("11445") int cifsPort,
            @Optional("11137") int namingPort,
            @Optional("11138") int datagramPort,
            @Optional("jLAN") String shareName,
            @Optional("target/it") String localSharePath)
                    throws IOException, InvalidConfigurationException, SMBException, InterruptedException
    {
        ServerConfiguration server = new ServerConfiguration(hostname);
        CIFSConfigSection cifs = new CIFSConfigSection(server);
        cifs.setServerName(hostname);
        cifs.setDomainName(workgroup);
        cifs.setBroadcastMask(broadcastMask);
        DialectSelector dialectSelector = new DialectSelector();
        dialectSelector.AddDialect(Dialect.LanMan1);
        dialectSelector.AddDialect(Dialect.LanMan2);
        dialectSelector.AddDialect(Dialect.LanMan2_1);
        dialectSelector.AddDialect(Dialect.Core);
        dialectSelector.AddDialect(Dialect.CorePlus);
        dialectSelector.AddDialect(Dialect.DOSLanMan1);
        dialectSelector.AddDialect(Dialect.DOSLanMan2);
        dialectSelector.AddDialect(Dialect.NT);

        cifs.setEnabledDialects(dialectSelector);
        cifs.setComment(comment);

        cifs.setWin32NetBIOS(true);
        cifs.setWin32HostAnnounceInterval(announceInterval);
        cifs.setHostAnnounceInterval(announceInterval);

        cifs.setNetBIOSSMB(true);
        cifs.setSessionPort(sessionPort);
        cifs.setNameServerPort(namingPort);
        cifs.setDatagramPort(datagramPort);

        cifs.setTcpipSMB(true);
        cifs.setTcpipSMBPort(cifsPort);

        cifs.setNetBIOSDebug(true);
        cifs.setHostAnnounceDebug(true);
        cifs.setSessionDebugFlags(SMBSrvSession.DBG_NEGOTIATE & SMBSrvSession.DBG_SOCKET & SMBSrvSession.DBG_TREE & SMBSrvSession.DBG_STATE);

        EnterpriseCifsAuthenticator authenticator = new EnterpriseCifsAuthenticator();
        authenticator.setAccessMode(SecurityMode.UserMode);
        authenticator.setDebug(true);
        cifs.setAuthenticator(authenticator);

        FilesystemsConfigSection fileSystem = new FilesystemsConfigSection(server);
        DiskDeviceContext ctx = new DiskDeviceContext();
        ctx.setConfigurationParameters(new ConfigElement("LocalPath", localSharePath + "/scratch"));
        EnhJavaFileDiskDriver driver = new EnhJavaFileDiskDriver();
        SharedDevice share = new DiskSharedDevice(shareName, driver, ctx);
        fileSystem.addShare(share);
        SecurityConfigSection security = new SecurityConfigSection(server);
        UserAccountList users = new UserAccountList();
        UserAccount admin = new UserAccount("admin", "admin");
        admin.setComment("System Administrator");
        users.addUser(admin);
        security.setUserAccounts(users);
        
        CoreServerConfigSection coreSettings = new CoreServerConfigSection(server);
        coreSettings.setMemoryPool(DefaultMemoryPoolBufSizes, DefaultMemoryPoolInitAlloc, DefaultMemoryPoolMaxAlloc);
        coreSettings.setThreadPool(DefaultThreadPoolInit, DefaultThreadPoolMax);
        GlobalConfigSection global = new GlobalConfigSection(server);
        global.setTimeZone(TimeZone.getDefault().toString());

        server.addConfigSection(coreSettings);
        server.addConfigSection(security);
        server.addConfigSection(fileSystem);
        server.addConfigSection(cifs);
        server.addConfigSection(global);
        SMBServer smbServer;
        smbServer = new SMBServer(server);
        smbServer.addServerListener(new ServerListener() {
            @Override
            public void serverStatusEvent(NetworkServer server, int event) {
                if (event == ServerListener.ServerActive) {
                    try {
                        CIFSDiskSession session = ParameterizedJcifsTest.createCifsSess(hostname, shareName, "admin", "admin", cifsPort);
                    } catch (IOException | SMBException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        smbServer.startServer();
        smbServer.shutdownServer(false);
    }
}
