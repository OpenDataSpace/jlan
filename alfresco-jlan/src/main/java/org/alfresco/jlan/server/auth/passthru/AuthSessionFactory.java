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

package org.alfresco.jlan.server.auth.passthru;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Vector;

import org.alfresco.jlan.netbios.NetBIOSName;
import org.alfresco.jlan.netbios.NetBIOSNameList;
import org.alfresco.jlan.netbios.NetBIOSSession;
import org.alfresco.jlan.netbios.NetworkSession;
import org.alfresco.jlan.netbios.RFCNetBIOSProtocol;
import org.alfresco.jlan.server.auth.PasswordEncryptor;
import org.alfresco.jlan.smb.DataType;
import org.alfresco.jlan.smb.Dialect;
import org.alfresco.jlan.smb.DialectSelector;
import org.alfresco.jlan.smb.PCShare;
import org.alfresco.jlan.smb.PacketType;
import org.alfresco.jlan.smb.Protocol;
import org.alfresco.jlan.smb.SMBException;
import org.alfresco.jlan.util.IPAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The AuthSessionFactory static class is used to create sessions to remote shared resources using the SMB/CIFS protocol. A PCShare object is used to specify
 * the remote node and share details, as well as required access control details.
 *
 * @author gkspencer
 */
public final class AuthSessionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthSessionFactory.class);

    // Default SMB dialect list
    private static DialectSelector m_defDialects;

    // Session index, used to make each session request call id unique
    private static int m_sessIdx = 1;

    // Default session packet buffer size
    private static int m_defPktSize = 4096 + RFCNetBIOSProtocol.HEADER_LEN;

    // List of local TCP/IP addresses
    private static InetAddress[] m_localAddrList;

    // Password encryptor
    private static PasswordEncryptor m_encryptor = new PasswordEncryptor();

    // If this is an evaluation version then only 5 sessions may be opened before an exception is
    // thrown.
    //
    // FULL VERSION = -1
    // DEMO VERSION = 5

    static {
        // Initialize the default dialect list
        m_defDialects = new DialectSelector();
        m_defDialects.AddDialect(Dialect.Core);
        m_defDialects.AddDialect(Dialect.CorePlus);
        m_defDialects.AddDialect(Dialect.DOSLanMan1);
        m_defDialects.AddDialect(Dialect.DOSLanMan2);
        m_defDialects.AddDialect(Dialect.LanMan1);
        m_defDialects.AddDialect(Dialect.LanMan2);
        m_defDialects.AddDialect(Dialect.LanMan2_1);
        m_defDialects.AddDialect(Dialect.NT);
    }

    // Default user name, password and domain used by methods that create their own connections.
    private static String m_defUserName = "";
    private static String m_defPassword = "";
    private static String m_defDomain = "?";

    // Primary and secondary protocols to connect with
    private static int m_primaryProto = Protocol.TCPNetBIOS;
    private static int m_secondaryProto = Protocol.NativeSMB;

    // NetBIOS port to connect to when setting up a new session. The default port is 139.
    private static int m_netbiosPort = RFCNetBIOSProtocol.PORT;

    // NetBIOS name scope
    private static String m_netBIOSScopeId = null;

    /**
     * Build an SMB negotiate dialect packet.
     *
     * @param pkt
     *            SMBPacket to build the negotiate request
     * @param dlct
     *            SMB dialects to negotiate
     * @param pid
     *            Process id to be used by this new session
     */
    private final static Vector<String> BuildNegotiatePacket(final SMBPacket pkt, final DialectSelector dlct, final int pid) {
        // Initialize the SMB packet header fields
        pkt.setCommand(PacketType.Negotiate);
        pkt.setProcessId(pid);

        // If the NT dialect is enabled set the Unicode flag in the request flags
        if (dlct.hasDialect(Dialect.NT)) {
            pkt.setFlags2(SMBPacket.FLG2_UNICODE);
        }

        // Build the SMB dialect list
        final StringBuffer dia = new StringBuffer();
        final Vector<String> vec = new Vector<>();

        // Loop through all SMB dialect types and add the appropriate dialect strings
        // to the negotiate packet.
        int d = Dialect.Core;
        while (d < Dialect.Max) {
            // Check if the current dialect is selected
            if (dlct.hasDialect(d)) {
                // Search the SMB dialect type string list and add all strings for the
                // current dialect
                for (int i = 0; i < Dialect.NumberOfDialects(); i++) {
                    // Check if the current dialect string should be added to the list
                    if (Dialect.DialectType(i) == d) {
                        // Get the current SMB dialect string
                        final String curDialect = Dialect.DialectString(i);
                        vec.addElement(curDialect);

                        // Add the current SMB dialect type string to the negotiate packet
                        dia.append(DataType.Dialect);
                        dia.append(curDialect);
                        dia.append((char) 0x00);
                    }
                }
            }

            // Update the current dialect type
            d++;
        }

        // Copy the dialect strings to the SMB packet
        pkt.setBytes(dia.toString().getBytes());

        // Return the dialect strings
        return vec;
    }

    /**
     * Return the default SMB packet size
     *
     * @return Default SMB packet size to allocate.
     */
    protected final static int DefaultPacketSize() {
        return m_defPktSize;
    }

    /**
     * Return the list of SMB dialects that will be negotiated when a new session is created.
     *
     * @return DialectSelector List of enabled SMB dialects.
     */
    public final static DialectSelector getDefaultDialects() {
        return m_defDialects;
    }

    /**
     * Return the default domain name
     *
     * @return String
     */
    public static String getDefaultDomain() {
        return m_defDomain;
    }

    /**
     * Return the default password.
     *
     * @return java.lang.String
     */
    public static String getDefaultPassword() {
        return m_defPassword;
    }

    /**
     * Return the default user name.
     *
     * @return java.lang.String
     */
    public static String getDefaultUserName() {
        return m_defUserName;
    }

    /**
     * Return the NetBIOS scope id, or null if not set
     *
     * @return String
     */
    public static String getNetBIOSNameScope() {
        return m_netBIOSScopeId;
    }

    /**
     * Return the NetBIOS socket number that new sessions are connected to.
     *
     * @return int NetBIOS session socket number.
     */
    public static int getNetBIOSPort() {
        return m_netbiosPort;
    }

    /**
     * Return the primary connection protocol (either Protocol.TCPNetBIOS or Protocol.NativeSMB)
     *
     * @return int
     */
    public static final int getPrimaryProtocol() {
        return m_primaryProto;
    }

    /**
     * Return the secondary connection protocol (Protocol.TCPNetBIOS, Protocol.NativeSMB or Protocol.None)
     *
     * @return int
     */
    public static final int getSecondaryProtocol() {
        return m_secondaryProto;
    }

    /**
     * Return the next session id
     *
     * @return int
     */
    private static synchronized int getSessionId() {
        final int sessId = m_sessIdx++ + (NetBIOSSession.getJVMIndex() * 100);
        return sessId;
    }

    /**
     * Get the list of local TCP/IP addresses
     *
     * @return InetAddress[]
     */
    private static synchronized InetAddress[] getLocalTcpipAddresses() {
        // Get the list of local TCP/IP addresses
        if (m_localAddrList == null) {
            try {
                m_localAddrList = InetAddress.getAllByName(InetAddress.getLocalHost().getHostName());
            } catch (final UnknownHostException ex) {
            }
        }

        // Return the address list
        return m_localAddrList;
    }

    /**
     * Determine if the NetBIOS name scope is set
     *
     * @return boolean
     */
    public final static boolean hasNetBIOSNameScope() {
        return m_netBIOSScopeId != null ? true : false;
    }

    /**
     * Open a session to a remote server, negotiate an SMB dialect and get the returned challenge key. Returns an AuthenticateSession which can then be used to
     * provide passthru authentication.
     *
     * @param shr
     *            Remote server share and access control details.
     * @param tmo
     *            Timeout value in milliseconds
     * @return AuthenticateSession for the new session, else null.
     * @exception java.io.IOException
     *                If an I/O error occurs.
     * @exception java.net.UnknownHostException
     *                Remote node is unknown.
     * @exception SMBException
     *                Failed to setup a new session.
     */
    public static AuthenticateSession OpenAuthenticateSession(final PCShare shr, final int tmo)
            throws java.io.IOException, java.net.UnknownHostException, SMBException {

        // Open an authentication session

        return OpenAuthenticateSession(shr, tmo, null);
    }

    /**
     * Open a session to a remote server, negotiate an SMB dialect and get the returned challenge key. Returns an AuthenticateSession which can then be used to
     * provide passthru authentication.
     *
     * @param shr
     *            Remote server share and access control details.
     * @param tmo
     *            Timeout value in milliseconds
     * @param dia
     *            SMB dialects to negotiate for this session.
     * @return AuthenticateSession for the new session, else null.
     * @exception java.io.IOException
     *                If an I/O error occurs.
     * @exception java.net.UnknownHostException
     *                Remote node is unknown.
     * @exception SMBException
     *                Failed to setup a new session.
     */
    public static AuthenticateSession OpenAuthenticateSession(final PCShare shr, final int tmo, final DialectSelector dia)
            throws java.io.IOException, java.net.UnknownHostException, SMBException {
        // Build a unique caller name
        final int pid = getSessionId();

        final StringBuffer nameBuf = new StringBuffer(InetAddress.getLocalHost().getHostName() + "_" + pid);
        final String localName = nameBuf.toString();

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("** New auth session from " + localName + " to " + shr.toString());

            // Display the Java system variables
            LOGGER.info("** os.arch = " + System.getProperty("os.arch") + ", java.version: " + System.getProperty("java.version"));
        }

        // Connect to the requested server
        NetworkSession netSession = null;
        switch (getPrimaryProtocol()) {
            // NetBIOS connection
            case Protocol.TCPNetBIOS:
                netSession = connectNetBIOSSession(shr.getNodeName(), localName, tmo);
                break;

            // Native SMB connection
            case Protocol.NativeSMB:
                netSession = connectNativeSMBSession(shr.getNodeName(), localName, tmo);
                break;
        }

        // If the connection was not made using the primary protocol try the secondary protocol, if configured
        if (netSession == null) {
            // Try the secondary protocol
            switch (getSecondaryProtocol()) {
                // NetBIOS connection
                case Protocol.TCPNetBIOS:
                    netSession = connectNetBIOSSession(shr.getNodeName(), localName, tmo);
                    break;

                // Native SMB connection
                case Protocol.NativeSMB:
                    netSession = connectNativeSMBSession(shr.getNodeName(), localName, tmo);
                    break;
            }
        }

        // Check if we connected to the remote host
        if (netSession == null) {
            throw new IOException("Failed to connect to host, " + shr.getNodeName());
        }

        LOGGER.info("** Connected session, protocol : {}", netSession.getProtocolName());

        // Build a protocol negotiation SMB packet, and send it to the remote
        // file server.
        final SMBPacket pkt = new SMBPacket();
        DialectSelector selDialect = dia;

        if (selDialect == null) {
            // Use the default SMB dialect list
            selDialect = new DialectSelector();
            selDialect.copyFrom(m_defDialects);
        }

        // Build the negotiate SMB dialect packet and exchange with the remote server
        final Vector<String> diaList = BuildNegotiatePacket(pkt, selDialect, pid);
        pkt.ExchangeLowLevelSMB(netSession, pkt, true);

        // Determine the selected SMB dialect

        final String diaStr = diaList.elementAt(pkt.getParameter(0));
        final int dialectId = Dialect.DialectType(diaStr);

        LOGGER.info("** SessionFactory: Negotiated SMB dialect {}", diaStr);

        if (dialectId == Dialect.Unknown) {
            throw new java.io.IOException("Unknown SMB dialect");
        }

        // Create the authenticate session
        final AuthenticateSession authSess = new AuthenticateSession(shr, netSession, dialectId, pkt);
        return authSess;
    }

    /**
     * Set the default domain.
     *
     * @param domain
     *            String
     */
    public static void setDefaultDomain(final String domain) {
        m_defDomain = domain;
    }

    /**
     * Set the default password.
     *
     * @param pwd
     *            java.lang.String
     */
    public static void setDefaultPassword(final String pwd) {
        m_defPassword = pwd;
    }

    /**
     * Set the default user name.
     *
     * @param user
     *            java.lang.String
     */
    public static void setDefaultUserName(final String user) {
        m_defUserName = user;
    }

    /**
     * Set the NetBIOS socket number to be used when setting up new sessions. The default socket is 139.
     *
     * @param port
     *            int
     */
    public static void setNetBIOSPort(final int port) {
        m_netbiosPort = port;
    }

    /**
     * Set the NetBIOS scope id
     *
     * @param scope
     *            String
     */
    public static void setNetBIOSNameScope(final String scope) {
        if (scope != null && scope.startsWith(".")) {
            m_netBIOSScopeId = scope.substring(1);
        } else {
            m_netBIOSScopeId = scope;
        }
    }

    /**
     * Set the protocol connection order
     *
     * @param pri
     *            Primary connection protocol
     * @param sec
     *            Secondary connection protocol, or none
     * @return boolean
     */
    public static final boolean setProtocolOrder(final int pri, final int sec) {
        // Primary protocol must be specified
        if (pri != Protocol.TCPNetBIOS && pri != Protocol.NativeSMB) {
            return false;
        }

        // Primary and secondary must be different
        if (pri == sec) {
            return false;
        }

        // Save the settings
        m_primaryProto = pri;
        m_secondaryProto = sec;

        return true;
    }

    /**
     * Set the subnet mask string for network broadcast requests
     *
     * If the subnet mask is not set a default broadcast mask for the TCP/IP address class will be used.
     *
     * @param subnet
     *            Subnet mask string, in 'nnn.nnn.nnn.nnn' format.
     */
    public final static void setSubnetMask(final String subnet) {
        NetBIOSSession.setDefaultSubnetMask(subnet);
    }

    /**
     * Connect a NetBIOS network session
     *
     * @param toName
     *            Host name/address to connect to
     * @param fromName
     *            Local host name/address
     * @param tmo
     *            Timeout in seconds
     * @return NetworkSession
     * @exception IOException
     *                If a network error occurs
     */
    private static final NetworkSession connectNetBIOSSession(String toName, String fromName, final int tmo) throws IOException {
        // Connect to the requested server
        final NetBIOSSession nbSession = new NetBIOSSession(tmo, getNetBIOSPort(), RFCNetBIOSProtocol.NAME_PORT);

        // Check if the remote host is specified as a TCP/IP address
        String toAddr = null;
        NetBIOSName nbName = null;

        if (IPAddress.isNumericAddress(toName)) {
            try {
                // Get a list of NetBIOS names from the remote host
                toAddr = toName;
                final NetBIOSNameList nameList = NetBIOSSession.FindNamesForAddress(toAddr);

                // Find the server service
                nbName = nameList.findName(NetBIOSName.FileServer, false);
                if (nbName == null) {
                    throw new IOException("Server service not running");
                }

                // Set the remote name
                toName = nbName.getName();
            } catch (final UnknownHostException ex) {
                return null;
            }
        } else {
            // Find the remote host and get a list of the network addresses it is using
            nbName = NetBIOSSession.FindName(toName, NetBIOSName.FileServer, 500);
        }

        // Check if the NetBIOS name scope has been set, if so then update the names to add the scope id
        if (hasNetBIOSNameScope()) {
            // Add the NetBIOS scope id to the to/from NetBIOS names
            toName = toName + "." + getNetBIOSNameScope();
            fromName = fromName + "." + getNetBIOSNameScope();
        }

        // If the NetBIOS name has more than one TCP/IP address then find the best match for the client and
        // try to connect on that address first, if that fails then we will have to try each address in turn.
        if (nbName.numberOfAddresses() > 1) {
            // Get the local TCP/IP address list and search for a best match address to connect to the server on
            final InetAddress[] addrList = getLocalTcpipAddresses();
            final int addrIdx = nbName.findBestMatchAddress(addrList);
            if (addrIdx != -1) {
                try {
                    // Get the server IP address
                    final String ipAddr = nbName.getIPAddressString(addrIdx);

                    LOGGER.info("** Server is multi-homed, trying to connect to {}", ipAddr);

                    // Open the session to the remote host

                    nbSession.Open(toName, fromName, ipAddr);

                    // Check if the session is connected

                    if (nbSession.isConnected() == false) {

                        // Close the session

                        try {
                            nbSession.Close();
                        } catch (final Exception ex) {
                        }
                    } else if (nbSession.isConnected()) {
                        LOGGER.info("** Connected to address {}", ipAddr);
                    }
                } catch (final IOException ex) {
                }
            }
        }

        if (nbSession.isConnected() == false && nbName.numberOfAddresses() > 1) {
            LOGGER.info("** Server is multi-homed, trying all addresses");
        }

        // Loop through the available addresses for the remote file server until we get a successful
        // connection, or all addresses have been used
        IOException lastException = null;
        int addrIdx = 0;
        while (nbSession.isConnected() == false && addrIdx < nbName.numberOfAddresses()) {
            try {
                // Get the server IP address
                final String ipAddr = nbName.getIPAddressString(addrIdx++);
                LOGGER.info("** Trying address {}", ipAddr);

                // Open the session to the remote host
                nbSession.Open(toName, fromName, ipAddr);

                // Check if the session is connected
                if (nbSession.isConnected() == false) {
                    // Close the session
                    try {
                        nbSession.Close();
                    } catch (final Exception ex) {
                    }
                } else if (nbSession.isConnected()) {
                    LOGGER.info("** Connected to address {}", ipAddr);
                }
            } catch (final IOException ex) {

                // Save the last exception

                lastException = ex;
            }
        }

        // Check if the session is connected

        if (nbSession.isConnected() == false) {

            // If there is a saved exception rethrow it

            if (lastException != null) {
                throw lastException;
            }

            // Indicate that the session was not connected

            return null;
        }

        // Return the network session

        return nbSession;
    }

    /**
     * Connect a native SMB network session
     *
     * @param toName
     *            Host name/address to connect to
     * @param fromName
     *            Local host name/address
     * @param tmo
     *            Timeout in seconds
     * @return NetworkSession
     * @exception IOException
     *                If a network error occurs
     */
    private static final NetworkSession connectNativeSMBSession(final String toName, final String fromName, final int tmo) throws IOException {
        // Connect to the requested server
        TcpipSMBNetworkSession tcpSession = new TcpipSMBNetworkSession(tmo);
        try {
            // Open the session
            tcpSession.Open(toName, fromName, null);

            // Check if the session is connected
            if (tcpSession.isConnected() == false) {
                // Close the session
                try {
                    tcpSession.Close();
                } catch (final Exception ex) {
                }

                // Return a null session
                return null;
            }
        } catch (final Exception ex) {
            try {
                tcpSession.Close();
            } catch (final Exception ex2) {
            }
            tcpSession = null;
        }

        // Return the network session
        return tcpSession;
    }
}
