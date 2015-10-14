/*
 * Copyright (C) 2005-2010 Alfresco Software Limited.
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
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.alfresco.jlan.smb.server.PacketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel Session Handler Class
 *
 * <p>
 * Base class for channel based session handler implementations.
 *
 * @author gkspencer
 */
public abstract class ChannelSessionHandler extends SessionHandlerBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelSessionHandler.class);

    // Server socket channel for receiving incoming connections
    private ServerSocketChannel m_srvSockChannel;

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
    public ChannelSessionHandler(final String name, final String protocol, final NetworkServer server, final InetAddress addr, final int port) {
        super(name, protocol, server, addr, port);
    }

    /**
     * Return the server socket channel
     *
     * @return ServerSocketChannel
     */
    public final ServerSocketChannel getSocketChannel() {
        return m_srvSockChannel;
    }

    /**
     * Initialize the session handler
     *
     * @param server
     *            NetworkServer
     */
    @Override
    public void initializeSessionHandler(final NetworkServer server) throws IOException {
        // Create the server socket channel
        m_srvSockChannel = ServerSocketChannel.open();

        // Open the server socket
        InetSocketAddress sockAddr = null;

        if (hasBindAddress()) {
            sockAddr = new InetSocketAddress(getBindAddress(), getPort());
        } else {
            sockAddr = new InetSocketAddress(getPort());
        }

        // Bind the socket
        m_srvSockChannel.socket().bind(sockAddr, getListenBacklog());

        // Set the allocated port
        if (getPort() == 0) {
            setPort(m_srvSockChannel.socket().getLocalPort());
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
            if (m_srvSockChannel != null) {
                m_srvSockChannel.close();
            }
        } catch (final SocketException ex) {
        } catch (final Exception ex) {
        }
    }

    /**
     * Create a packet handler for the new client socket connection
     *
     * @param sockChannel
     *            SocketChannel
     * @return PacketHandler
     * @exception IOException
     */
    public abstract PacketHandler createPacketHandler(SocketChannel sockChannel) throws IOException;
}
