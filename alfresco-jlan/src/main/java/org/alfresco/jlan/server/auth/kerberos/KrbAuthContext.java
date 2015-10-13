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

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosKey;

import org.alfresco.jlan.server.auth.AuthContext;
import org.alfresco.jlan.server.auth.asn.DER;
import org.alfresco.jlan.server.auth.asn.DERBuffer;
import org.alfresco.jlan.server.auth.asn.DERObject;
import org.alfresco.jlan.server.auth.asn.DEROid;
import org.alfresco.jlan.util.HexDump;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.security.krb5.EncryptedData;
import sun.security.krb5.EncryptionKey;

/**
 * Kerberos Authentication Context Class
 *
 * @author gkspencer
 */
public class KrbAuthContext extends AuthContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(KrbAuthContext.class);
    
    // AP-REQ
    private KerberosApReq m_apReq;

    // Encrypted Kerberos ticket part and authenticator
    private EncKrbTicket m_encTkt;
    private KrbAuthenticator m_krbAuth;

    // Enable debug output
    private boolean m_debug;

    /**
     * Default constructor
     */
    public KrbAuthContext() {
    }

    /**
     * Parse the AP-REQ blob to extract the encypted ticket and authenticator
     *
     * @param subj
     *            Subject
     * @param apReq
     *            KerberosApReq
     * @exception IOException
     */
    public final void parseKerberosApReq(final Subject subj, final KerberosApReq apReq) throws IOException {
        // Save the AP-REQ
        m_apReq = apReq;

        // Parse the Kerberos ticket
        final KrbTicket krbTkt = new KrbTicket(apReq.getTicket());
        if (LOGGER.isDebugEnabled() && hasDebug()) {
            LOGGER.debug("Kerberos ticket - " + krbTkt);
        }

        // Get the private key or session key
        final Set<KerberosKey> krbKeySet = subj.getPrivateCredentials(KerberosKey.class);

        final Iterator<KerberosKey> keyIter = krbKeySet.iterator();
        while (keyIter.hasNext()) {
            // Get the current key
            final KerberosKey krbKey = keyIter.next();

            if (krbKey.getKeyType() == krbTkt.getEncryptedType()) {
                // Try the current encryption key
                final EncryptionKey encKey = new EncryptionKey(krbKey.getEncoded(), krbKey.getKeyType(), new Integer(2));

                // Decrypt the encrypted ticket
                try {
                    // Decrypt the encrypted part of the Kerberos ticket
                    final EncryptedData encPart = new EncryptedData(krbTkt.getEncryptedType(),
                            krbTkt.getEncryptedPartKeyVersion() != -1 ? new Integer(krbTkt.getEncryptedPartKeyVersion()) : null, krbTkt.getEncryptedPart());
                    final byte[] decPart = encPart.decrypt(encKey, 2);

                    if (LOGGER.isDebugEnabled() && hasDebug()) {
                        LOGGER.debug("Decrypted ticket = Len=" + decPart.length + ", key=[Type=" + encKey.getEType() + ", Kvno=" + encKey.getKeyVersionNumber()
                                + ", Key=" + HexDump.hexString(encKey.getBytes()) + "]");
                    }

                    final DERBuffer derBuf = new DERBuffer(decPart);
                    final byte[] encTktByts = derBuf.unpackApplicationSpecificBytes();

                    if (encTktByts != null) {
                        // Create the encrypted Kerberos ticket part
                        m_encTkt = new EncKrbTicket(encTktByts);

                        if (LOGGER.isDebugEnabled() && hasDebug()) {
                            LOGGER.debug("Enc Krb Ticket Part = " + m_encTkt);
                        }
                    }
                } catch (final Exception ex) {
                    if (LOGGER.isDebugEnabled() && hasDebug()) {
                        LOGGER.debug("Ticket Error", ex);
                    }
                }
            }
        }

        // Use the session key to decrypt the authenticator
        if (m_encTkt != null) {
            // Create the session key
            final EncryptionKey encKey = new EncryptionKey(m_encTkt.getEncryptionKeyType(), m_encTkt.getEncryptionKey());

            // Decrypt the authenticator
            try {
                // Decrypt the authenticator
                final EncryptedData encPart = new EncryptedData(apReq.getAuthenticatorEncType(),
                        apReq.getAuthenticatorKeyVersion() != -1 ? new Integer(apReq.getAuthenticatorKeyVersion()) : null, apReq.getAuthenticator());
                final byte[] decPart = encPart.decrypt(encKey, 11);

                if (LOGGER.isDebugEnabled() && hasDebug()) {
                    LOGGER.debug("Decrypted authenticator = Len=" + decPart.length + ", key=[Type=" + encKey.getEType() + ", Kvno="
                            + encKey.getKeyVersionNumber() + ", Key=" + HexDump.hexString(encKey.getBytes()) + "]");
                }

                final DERBuffer derBuf = new DERBuffer(decPart);
                final byte[] krbAuthByts = derBuf.unpackApplicationSpecificBytes();

                if (krbAuthByts != null) {
                    // Parse the authenticator
                    m_krbAuth = new KrbAuthenticator(krbAuthByts);
                    if (LOGGER.isDebugEnabled() && hasDebug()) {
                        LOGGER.debug("Krb Authenticator = " + m_krbAuth);
                    }
                }
            } catch (final Exception ex) {
                if (LOGGER.isDebugEnabled() && hasDebug()) {
                    LOGGER.debug("Auth Error", ex);
                }
            }
        } else {
            throw new IOException("Failed to decrypt Kerberos ticket");
        }
    }

    /**
     * Parse the Kerberos AP-REP and return the updated response
     *
     * @param respTok
     *            byte[]
     * @return byte[]
     * @exception Exception
     */
    public final byte[] parseKerberosApRep(final byte[] respTok) throws Exception {
        // Parse the response token
        DERBuffer derBuf = new DERBuffer(respTok);
        byte[] aprepBlob = null;

        // Get the application specific object
        DEROid oid = null;
        int tokId = 0;

        final DERObject derObj = derBuf.unpackApplicationSpecific();
        if (derObj != null) {
            // Read the OID and token id
            if (derObj instanceof DEROid) {
                oid = (DEROid) derObj;
            }

            tokId = derBuf.unpackByte();
            tokId += derBuf.unpackByte() >> 8;

            // Read the AP-REP object
            if (DER.isApplicationSpecific(derBuf.peekType())) {
                aprepBlob = derBuf.unpackApplicationSpecificBytes();
            }
        }

        // Parse the Kerberos AP-REP
        final KerberosApRep krbApRep = new KerberosApRep(aprepBlob);

        if (LOGGER.isDebugEnabled() && hasDebug()) {
            LOGGER.debug("Kerberos AP-REP - " + krbApRep);
        }

        // Create the session key
        final EncryptionKey encKey = new EncryptionKey(m_encTkt.getEncryptionKeyType(), m_encTkt.getEncryptionKey());

        // Decrypt the AP-REP
        byte[] updRespTok = null;

        // Decrypt the AP-REP
        EncryptedData encPart = new EncryptedData(krbApRep.getEncryptionType(), krbApRep.getKeyVersion() != -1 ? new Integer(krbApRep.getKeyVersion()) : null,
                krbApRep.getEncryptedPart());
        byte[] decPart = encPart.decrypt(encKey, 12);

        if (LOGGER.isDebugEnabled() && hasDebug()) {
            LOGGER.debug("Decrypted AP-REP Len=" + decPart.length + ", key=[Type=" + encKey.getEType() + ", Key=" + HexDump.hexString(encKey.getBytes()) + "]");
        }

        derBuf = new DERBuffer(decPart);
        final byte[] encApRepByts = derBuf.unpackApplicationSpecificBytes();

        if (encApRepByts != null) {
            // Parse the AP-REP encrypted part
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("EncApRep bytes: ");
                LOGGER.debug(HexDump.DumpToString(decPart, decPart.length, 0));
            }

            final EncApRepPart encApRep = new EncApRepPart(encApRepByts);
            if (LOGGER.isDebugEnabled() && hasDebug()) {
                LOGGER.debug("EncApRep = " + encApRep);
            }

            // Add the sub-key from the client AP-REQ

            if (encApRep.getSubKey() == null) {
                // Use the subkey sent by the client
                encApRep.setSubkey(m_krbAuth.getSubKeyType(), m_krbAuth.getSubKey());

                if (LOGGER.isDebugEnabled() && hasDebug()) {
                    LOGGER.debug("Using client sub-key, type=" + m_krbAuth.getSubKeyType() + ", key=" + HexDump.hexString(m_krbAuth.getSubKey()));
                }

                // Rebuild the ASN.1 encoded AP-REP part
                decPart = encApRep.encodeApRep();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Re-encoded EncapRep bytes:");
                    LOGGER.debug(HexDump.DumpToString(decPart, decPart.length, 0));
                }

                // Encrypt the updated AP-REP part
                encPart = new EncryptedData(encKey, decPart, 12);

                // Rebuild the Kerberos AP-REP

                krbApRep.setEncryptedPart(krbApRep.getEncryptionType(), encPart.getBytes(), krbApRep.getKeyVersion());

                // ASN.1 encode the AP-REP

                aprepBlob = krbApRep.encodeApRep();

                // Rebuild the response token
                //
                // Pack the OID
                final DERBuffer oidBuf = new DERBuffer();
                oid.derEncode(oidBuf);
                final byte[] oidBytes = oidBuf.getBytes();
                updRespTok = new byte[aprepBlob.length + 2 + oidBytes.length];

                int pos = 0;
                System.arraycopy(oidBytes, 0, updRespTok, pos, oidBytes.length);
                pos += oidBytes.length;

                updRespTok[pos++] = (byte) (tokId & 0xFF);
                updRespTok[pos++] = (byte) ((tokId >> 8) & 0xFF);

                System.arraycopy(aprepBlob, 0, updRespTok, pos, aprepBlob.length);

                // Wrap up as an application specific object
                final DERBuffer appBuf = new DERBuffer();
                appBuf.packApplicationSpecific(updRespTok);
                updRespTok = appBuf.getBytes();
            }
        }

        // Return the updated response token
        return updRespTok;
    }

    /**
     * Check if debug output is enabled
     *
     * @return boolean
     */
    public final boolean hasDebug() {
        return m_debug;
    }

    /**
     * Enable/disable debug output
     *
     * @param ena
     *            boolean
     */
    public final void setDebug(final boolean ena) {
        m_debug = ena;
    }

    /**
     * Return the Kerberos authentication context details as a string
     *
     * @return String
     */
    @Override
    public String toString() {
        final StringBuilder str = new StringBuilder();
        str.append("[KrbAuthCtx:AP-REQ=");
        str.append(m_apReq);
        str.append(",EncTkt=");
        str.append(m_encTkt);
        str.append(",KrbAuth=");
        str.append(m_krbAuth);
        str.append("]");
        return str.toString();
    }
}
