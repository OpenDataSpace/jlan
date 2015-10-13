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

package org.alfresco.jlan.app;

import org.alfresco.jlan.oncrpc.nfs.NFSConfigSection;
import org.alfresco.jlan.oncrpc.portmap.PortMapperServer;
import org.alfresco.jlan.server.config.ServerConfiguration;
import org.alfresco.jlan.util.ConsoleIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Portmapper service class
 *
 * @author gkspencer
 */
public class Portmap {
    private static final Logger LOGGER = LoggerFactory.getLogger(Portmap.class);

	/**
	 * Main application
	 *
	 * @param args String[]
	 */
	public static void main(String[] args) {
		try {
			// Create the default configuration
			ServerConfiguration srvConfig = new ServerConfiguration( "PORTMAP");
			NFSConfigSection nfsConfig = new NFSConfigSection(srvConfig);
			nfsConfig.setPortMapperDebug( true);
			
			// Create the port mapper service
			PortMapperServer portMapper = new PortMapperServer( srvConfig);

			// Start the port mapper
			portMapper.startServer();

			//  Wait while the server runs, user may stop server by typing a key
			boolean shutdown = false;

			while (shutdown == false) {
				//	Check if the user has requested a shutdown, if running interactively
				int inChar = ConsoleIO.readCharacter();
				if ( inChar == 'x' || inChar == 'X') {
					shutdown = true;
				}
				
				//	Sleep for a short while
				try {
					Thread.sleep(500);
				} catch (InterruptedException ex) {
				}
			}

			// Shutdown the port mapper service
			portMapper.shutdownServer( false);
		} catch (Exception ex) {
			LOGGER.error(ex.getMessage(), ex);
		}
	}
}
