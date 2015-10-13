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

package org.alfresco.jlan.server.auth.kerberos;

import java.security.PrivilegedAction;

import org.alfresco.jlan.server.auth.spnego.OID;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Session Setup Privileged Action Class
 *
 * <p>
 * Handle the processing of a received SPNEGO packet in the context of the CIFS server.
 *
 * @author gkspencer
 */
public class SessionSetupPrivilegedAction implements PrivilegedAction {
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionSetupPrivilegedAction.class);

    // Received security blob details
    private final byte[] m_secBlob;
    private final int m_secOffset;
    private final int m_secLen;

    // CIFS server account name
    private final String m_accountName;

    /**
     * Class constructor
     *
     * @param accountName
     *            String
     * @param secBlob
     *            byte[]
     */
    public SessionSetupPrivilegedAction(final String accountName, final byte[] secBlob) {
        m_accountName = accountName;
        m_secBlob = secBlob;
        m_secOffset = 0;
        m_secLen = secBlob.length;
    }

    /**
     * Class constructor
     *
     * @param accountName
     *            String
     * @param secBlob
     *            byte[]
     * @param secOffset
     *            int
     * @param secLen
     *            int
     */
    public SessionSetupPrivilegedAction(final String accountName, final byte[] secBlob, final int secOffset, final int secLen) {
        m_accountName = accountName;
        m_secBlob = secBlob;
        m_secOffset = secOffset;
        m_secLen = secLen;
    }

    /**
     * Run the privileged action
     */
    @Override
    public Object run() {
        KerberosDetails krbDetails = null;
        try {
            final GSSManager gssManager = GSSManager.getInstance();
            final GSSName serverGSSName = gssManager.createName(m_accountName, GSSName.NT_USER_NAME);
            final GSSCredential serverGSSCreds = gssManager.createCredential(serverGSSName, GSSCredential.INDEFINITE_LIFETIME, OID.KERBEROS5,
                    GSSCredential.ACCEPT_ONLY);

            final GSSContext serverGSSContext = gssManager.createContext(serverGSSCreds);

            // Accept the incoming security blob and generate the response blob
            final byte[] respBlob = serverGSSContext.acceptSecContext(m_secBlob, m_secOffset, m_secLen);

            // Create the Kerberos response details
            krbDetails = new KerberosDetails(serverGSSContext.getSrcName(), serverGSSContext.getTargName(), respBlob);
        } catch (final GSSException ex) {
            LOGGER.debug(ex.getMessage(), ex);
        }

        // Return the Kerberos response
        return krbDetails;
    }
}
