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

import org.alfresco.jlan.oncrpc.nfs.NFSConfigSection;
import org.alfresco.jlan.oncrpc.portmap.PortMapper;
import org.alfresco.jlan.server.NetworkServer;
import org.alfresco.jlan.server.config.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RPC Network Server Abstract Class
 *
 * <p>
 * Provides the base class for RPC servers (such as mount and NFS).
 *
 * @author gkspencer
 */
public abstract class RpcNetworkServer extends NetworkServer implements RpcProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(RpcNetworkServer.class);
    // RPC service register/unregsiter lock
    private static final Object _rpcRegisterLock = new Object();

    // Port mapper port
    private int m_portMapperPort = PortMapper.DefaultPort;

    // RPC registration port
    private int m_rpcRegisterPort;

    /**
     * Class constructor
     *
     * @param name
     *            String
     * @param config
     *            ServerConfiguration
     */
    public RpcNetworkServer(final String name, final ServerConfiguration config) {
        super(name, config);
        // Set the RPC registration port
        final NFSConfigSection nfsConfig = (NFSConfigSection) config.getConfigSection(NFSConfigSection.SectionName);

        if (nfsConfig != null) {
            m_rpcRegisterPort = nfsConfig.getRPCRegistrationPort();
        }
    }

    /**
     * Register a port/protocol for the RPC server
     *
     * @param mapping
     *            PortMapping
     * @throws IOException
     */
    protected final void registerRPCServer(final PortMapping mapping) throws IOException {
        // Call the main registration method
        final PortMapping[] mappings = new PortMapping[1];
        mappings[0] = mapping;
        registerRPCServer(mappings);
    }

    /**
     * Register a set of ports/protocols for the RPC server
     *
     * @param mappings
     *            PortMapping[]
     * @throws IOException
     */
    protected final void registerRPCServer(final PortMapping[] mappings) throws IOException {
        // Check if portmapper registration has been disabled
        if (m_portMapperPort == -1) {
            return;
        }

        // Connect to the local portmapper service to register the RPC service
        final InetAddress localHost = InetAddress.getByName("127.0.0.1");

        TcpRpcClient rpcClient = null;

        try {
            // Synchronize access to the register port
            synchronized (_rpcRegisterLock) {
                // Create the RPC client to talk to the portmapper/rpcbind service
                rpcClient = new TcpRpcClient(localHost, m_portMapperPort, localHost, m_rpcRegisterPort, 512);

                // Allocate RPC request and response packets
                final RpcPacket setPortRpc = new RpcPacket(512);
                RpcPacket rxRpc = new RpcPacket(512);

                // Loop through the port mappings and register each port with the portmapper service
                for (final PortMapping mapping : mappings) {
                    // Build the RPC request header
                    setPortRpc.buildRequestHeader(PortMapper.ProgramId, PortMapper.VersionId, PortMapper.ProcSet, 0, null, 0, null);

                    // Pack the request parameters and set the request length
                    setPortRpc.packPortMapping(mapping);
                    setPortRpc.setLength();

                    if (LOGGER.isInfoEnabled() && hasDebug()) {
                        LOGGER.info("[" + getProtocolName() + "] Register server RPC " + mapping + " ...");
                    }

                    // Send the RPC request and receive a response

                    rxRpc = rpcClient.sendRPC(setPortRpc, rxRpc);

                    // Check if the server has been registered successfully with the portmapper/rpcbind service

                    if (rxRpc != null && rxRpc.getAcceptStatus() == Rpc.StsSuccess) {

                        // Server registered successfully

                        if (LOGGER.isInfoEnabled() && hasDebug()) {
                            LOGGER.info("[" + getProtocolName() + "] Registered successfully, " + mapping);
                        }
                    } else {

                        // Indicate that the server registration failed

                        LOGGER.warn("[" + getProtocolName() + "] RPC Server registration failed for " + mapping);
                        LOGGER.warn("  Response:" + rxRpc);
                    }
                }

                // Close the connection to the portmapper

                rpcClient.closeConnection();
                rpcClient = null;
            }
        } catch (final Exception ex) {

            // Debug

            if (LOGGER.isInfoEnabled() && hasDebug()) {
                LOGGER.info("[" + getProtocolName() + "] Failed to register RPC service", ex);
            }
        } finally {

            // Make sure the RPC client is closed down

            if (rpcClient != null) {
                rpcClient.closeConnection();
            }
        }
    }

    /**
     * Unregister a port/protocol for the RPC server
     *
     * @param mapping
     *            PortMapping
     * @throws IOException
     */
    protected final void unregisterRPCServer(final PortMapping mapping) throws IOException {

        // Call the main unregister ports method

        final PortMapping[] mappings = new PortMapping[1];
        mappings[0] = mapping;

        unregisterRPCServer(mappings);
    }

    /**
     * Unregister a set of ports/protocols for the RPC server
     *
     * @param mappings
     *            PortMapping[]
     * @throws IOException
     */
    protected final void unregisterRPCServer(final PortMapping[] mappings) throws IOException {

        // Check if portmapper registration has been disabled

        if (m_portMapperPort == -1) {
            return;
        }

        // Connect to the local portmapper service to unregister the RPC service

        final InetAddress localHost = InetAddress.getByName("127.0.0.1");

        TcpRpcClient rpcClient = null;

        try {

            // Synchronize access to the register port

            synchronized (_rpcRegisterLock) {

                // Create the RPC client to talk to the portmapper/rpcbind service
                rpcClient = new TcpRpcClient(localHost, m_portMapperPort, localHost, m_rpcRegisterPort, 512);

                // Allocate RPC request and response packets
                final RpcPacket setPortRpc = new RpcPacket(512);
                RpcPacket rxRpc = new RpcPacket(512);

                // Loop through the port mappings and unregister each port with the portmapper service

                for (final PortMapping mapping : mappings) {
                    // Build the RPC request header
                    setPortRpc.buildRequestHeader(PortMapper.ProgramId, PortMapper.VersionId, PortMapper.ProcUnSet, 0, null, 0, null);

                    // Pack the request parameters and set the request length
                    setPortRpc.packPortMapping(mapping);
                    setPortRpc.setLength();

                    if (LOGGER.isInfoEnabled() && hasDebug()) {
                        LOGGER.info("[" + getProtocolName() + "] UnRegister server RPC " + mapping + " ...");
                    }

                    // Send the RPC request and receive a response
                    rxRpc = rpcClient.sendRPC(setPortRpc, rxRpc);

                    // Check if the server has been unregistered successfully with the portmapper/rpcbind service
                    if (rxRpc != null && rxRpc.getAcceptStatus() == Rpc.StsSuccess) {
                        // Server registered successfully
                        if (LOGGER.isInfoEnabled() && hasDebug()) {
                            LOGGER.info("[" + getProtocolName() + "] UnRegistered successfully, " + mapping);
                        }
                    } else {
                        // Indicate that the server registration failed
                        LOGGER.warn("[" + getProtocolName() + "] RPC Server unregistration failed for " + mapping);
                        LOGGER.warn("  Response:" + rxRpc);
                    }
                }

                // Close the connection to the portmapper
                rpcClient.closeConnection();
                rpcClient = null;
            }
        } catch (final Exception ex) {
            if (LOGGER.isInfoEnabled() && hasDebug()) {
                LOGGER.info("[" + getProtocolName() + "] Failed to unregister RPC service", ex);
            }
        } finally {
            // Make sure the RPC client is closed down
            if (rpcClient != null) {
                rpcClient.closeConnection();
            }
        }
    }

    /**
     * Set the port mapper port, or -1 to disable portmapper registration
     *
     * @param port
     *            int
     */
    public final void setPortMapper(final int port) {
        m_portMapperPort = port;
    }

    /**
     * Start the RPC server
     */
    @Override
    public abstract void startServer();

    /**
     * Shutdown the RPC server
     *
     * @param immediate
     *            boolean
     */
    @Override
    public abstract void shutdownServer(boolean immediate);

    /**
     * Process an RPC request
     *
     * @param rpc
     *            RpcPacket
     * @return RpcPacket
     * @throws IOException
     */
    @Override
    public abstract RpcPacket processRpc(RpcPacket rpc) throws IOException;
}
