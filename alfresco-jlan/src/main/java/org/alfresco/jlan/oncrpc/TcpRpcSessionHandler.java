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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Enumeration;
import java.util.Hashtable;

import org.alfresco.jlan.server.NetworkServer;
import org.alfresco.jlan.server.PacketHandlerInterface;
import org.alfresco.jlan.server.SocketSessionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TCP RPC Session Handler Class
 *
 * <p>
 * Receives session requests via a TCP socketRPC requests via a datagram and passes the request to the registered RPC server.
 *
 * @author gkspencer
 */
public class TcpRpcSessionHandler extends SocketSessionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(TcpRpcSessionHandler.class);

    // RPC server implementation that handles the RPC processing
    private final RpcProcessor m_rpcProcessor;

    // Maximum request size allowed
    private final int m_maxRpcSize;

    // List of active sessions
    private final Hashtable<Integer, TcpRpcPacketHandler> m_sessions;

    /**
     * Class constructor
     *
     * @param name
     *            String
     * @param protocol
     *            String
     * @param rpcServer
     *            RpcProcessor
     * @param server
     *            NetworkServer
     * @param addr
     *            InetAddress
     * @param port
     *            int
     * @param maxSize
     *            int
     */
    public TcpRpcSessionHandler(final String name, final String protocol, final RpcProcessor rpcServer, final NetworkServer server, final InetAddress addr,
            final int port, final int maxSize) {
        super(name, protocol, server, addr, port);
        // Set the RPC server implementation that will handle the actual requests
        m_rpcProcessor = rpcServer;

        // Set the maximum RPC request size allowed
        m_maxRpcSize = maxSize;

        // Create the active session list
        m_sessions = new Hashtable<Integer, TcpRpcPacketHandler>();
    }

    /**
     * Return the maximum RPC size allowed
     *
     * @return int
     */
    protected int getMaximumRpcSize() {
        return m_maxRpcSize;
    }

    /**
     * Return the RPC server used to process the requests
     *
     * @return RpcProcessor
     */
    protected final RpcProcessor getRpcProcessor() {
        return m_rpcProcessor;
    }

    /**
     * Accept an incoming session request
     *
     * @param sock
     *            Socket
     */
    @Override
    protected void acceptConnection(final Socket sock) {
        try {
            // Set the socket for no delay
            sock.setTcpNoDelay(true);

            // Create a packet handler for the new session and add to the active session list
            final int sessId = getNextSessionId();
            final TcpRpcPacketHandler pktHandler = createPacketHandler(sessId, sock);

            // Add the packet handler to the active session table
            m_sessions.put(new Integer(sessId), pktHandler);
            if (LOGGER.isInfoEnabled() && hasDebug()) {
                LOGGER.info("[" + getProtocolName() + "] Created new session id = " + sessId + ", from = " + sock.getInetAddress().getHostAddress() + ":"
                        + sock.getPort());
            }
        } catch (final IOException ex) {
        }
    }

    /**
     * Remove a session from the active session list
     *
     * @param sessId
     *            int
     */
    protected final void closeSession(final int sessId) {
        // Remove the specified session from the active session table
        final PacketHandlerInterface pktHandler = m_sessions.remove(new Integer(sessId));
        if (pktHandler != null) {
            // Close the session
            pktHandler.closePacketHandler();
        }
    }

    /**
     * Close the session handler, close all active sessions.
     *
     * @param server
     *            NetworkServer
     */
    @Override
    public void closeSessionHandler(final NetworkServer server) {
        super.closeSessionHandler(server);
        // Close all active sessions
        if (m_sessions.size() > 0) {
            // Enumerate the sessions
            final Enumeration<TcpRpcPacketHandler> enm = m_sessions.elements();
            while (enm.hasMoreElements()) {
                // Get the current packet handler
                final PacketHandlerInterface handler = enm.nextElement();
                handler.closePacketHandler();
            }

            // Clear the session list
            m_sessions.clear();
        }
    }

    /**
     * Create a packet handler for a new session
     *
     * @param sessId
     *            int
     * @param sock
     *            Socket
     * @return TcpRpcPacketHandler
     * @exception IOException
     */
    protected TcpRpcPacketHandler createPacketHandler(final int sessId, final Socket sock) throws IOException {
        // Create a single threaded TCP RPC packet handler
        return new TcpRpcPacketHandler(this, sessId, m_rpcProcessor, sock, getMaximumRpcSize());
    }
}
