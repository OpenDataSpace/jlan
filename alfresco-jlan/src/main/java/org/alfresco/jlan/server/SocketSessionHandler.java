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

package org.alfresco.jlan.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Socket Session Handler Class
 *
 * <p>
 * Implementation of a session handler that uses a Java socket to listen for incoming session requests.
 *
 * @author gkspencer
 */
public abstract class SocketSessionHandler extends SessionHandlerBase implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SocketSessionHandler.class);

    // Server socket to listen for incoming connections
    private ServerSocket m_srvSock;

    // Client socket read timeout
    private int m_clientSockTmo;

    /**
     * Class constructor
     *
     * @param name
     *            String
     * @param protocol
     *            String
     * @param server
     *            NetworkServer
     * @param addr
     *            InetAddress
     * @param port
     *            int
     */
    public SocketSessionHandler(final String name, final String protocol, final NetworkServer server, final InetAddress addr, final int port) {
        super(name, protocol, server, addr, port);
    }

    /**
     * Return the server socket
     *
     * @return ServerSocket
     */
    public final ServerSocket getSocket() {
        return m_srvSock;
    }

    /**
     * Return the client socket timeout, in milliseconds
     *
     * @return int
     */
    public final int getSocketTimeout() {
        return m_clientSockTmo;
    }

    /**
     * Set the client socket timeout, in milliseconds, zero for no timeout
     *
     * @param tmo
     *            int
     */
    public final void setSocketTimeout(final int tmo) {
        m_clientSockTmo = tmo;
    }

    /**
     * Initialize the session handler
     *
     * @param server
     *            NetworkServer
     */
    @Override
    public void initializeSessionHandler(final NetworkServer server) throws IOException {
        // Open the server socket
        if (hasBindAddress()) {
            m_srvSock = new ServerSocket(getPort(), getListenBacklog(), getBindAddress());
        } else {
            m_srvSock = new ServerSocket(getPort(), getListenBacklog());
        }

        // Set the allocated port
        if (getPort() == 0) {
            setPort(m_srvSock.getLocalPort());
        }

        if (hasDebug()) {
            LOGGER.info("[{}] Binding {} session handler to address : {}", getProtocolName(), getHandlerName(),
                    hasBindAddress() ? getBindAddress().getHostAddress() : "ALL");
        }
    }

    /**
     * Close the session handler
     *
     * @param server
     *            NetworkServer
     */
    @Override
    public void closeSessionHandler(final NetworkServer server) {
        // Request the main listener thread shutdown
        setShutdown(true);
        try {
            // Close the server socket to release any pending listen
            if (m_srvSock != null) {
                m_srvSock.close();
            }
        } catch (final SocketException ex) {
        } catch (final Exception ex) {
        }
    }

    /**
     * Socket listener thread
     */
    @Override
    public void run() {
        try {
            // Clear the shutdown flag
            clearShutdown();

            // Wait for incoming connection requests
            while (hasShutdown() == false) {
                if (hasDebug()) {
                    LOGGER.info("[{}] Waiting for session request ...", getProtocolName());
                }

                // Wait for a connection
                final Socket sessSock = m_srvSock.accept();
                if (hasDebug()) {
                    LOGGER.info("[{}] Session request received from {}", getProtocolName(), sessSock.getInetAddress().getHostAddress());
                }
                try {
                    // Process the new connection request
                    acceptConnection(sessSock);
                } catch (final Exception ex) {
                    if (hasDebug()) {
                        LOGGER.info("[" + getProtocolName() + "] Failed to create session, " + ex.toString());
                    }
                }
            }
        } catch (final SocketException ex) {
            // Do not report an error if the server has shutdown, closing the server socket
            // causes an exception to be thrown.
            if (hasShutdown() == false) {
                LOGGER.error("[" + getProtocolName() + "] Socket error : ", ex);
            }
        } catch (final Exception ex) {
            // Do not report an error if the server has shutdown, closing the server socket
            // causes an exception to be thrown.
            if (hasShutdown() == false) {
                LOGGER.error("[" + getProtocolName() + "] Server error : ", ex);
            }
        }
        if (hasDebug()) {
            LOGGER.info("[{}] {} session handler closed", getProtocolName(), getHandlerName());
        }
    }

    /**
     * Accept a new connection on the specified socket
     *
     * @param sock
     *            Socket
     */
    protected abstract void acceptConnection(Socket sock);
}
