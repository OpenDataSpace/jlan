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

package org.alfresco.jlan.netbios.win32;

/**
 * Winsock Error Codes Class
 *
 * <p>Contains a list of the error codes that the Win32 Winsock calls may generate,
 * and a method to convert to an error text string.
 *
 * @author gkspencer
 */
public enum WinsockError {
    WsaEIntr        (10004, "Interrupted function call"),
    WsaEAcces       (10013, "Permission denied"),
    WsaEFault       (10014, "Bad address"),
    WsaEInval       (10022, "Invalid argument"),
    WsaEMfile       (10024, "Too many open files"),
    WsaEWouldBlock  (10035, "Resource temporarily unavailable"),
    WsaEInProgress  (10036, "Operation now in progress"),
    WsaEAlready     (10037, "Operation already in progress"),
    WsaENotSock     (10038, "Socket operation on nonsocket"),
    WsaEDestAddrReq (10039, "Destination address required"),
    WsaEMsgSize     (10040, "Message too long"),
    WsaEPrototype   (10041, "Protocol wrong type for socket"),
    WsaENoProtoOpt  (10042, "Bad protocol option"),
    WsaEProtoNoSupp (10043, "Protocol not supported"),
    WsaESocktNoSupp (10044, "Socket type not supported"),
    WsaEOpNotSupp   (10045, "Operation not supported"),
    WsaEPFNoSupport (10046, "Protocol family not supported"),
    WsaEAFNoSupport (10047, "Address family not supported by protocol family"),
    WsaEAddrInUse   (10048,  "Address already in use"),
    WsaEAddrNotAvail(10049, "Cannot assign requested address"),
    WsaENetDown     (10050, "Network is down"),
    WsaENetUnReach  (10051, "Network is unreachable"),
    WsaENetReset    (10052, "Network dropped connection on reset"),
    WsaEConnAborted (10053, "Software caused connection abort"),
    WsaEConnReset   (10054, "Connection reset by peer"),
    WsaENoBufs      (10055, "No buffer space available"),
    WsaEIsConn      (10056, "Socket is already connected"),
    WsaENotConn     (10057, "Socket is not connected"),
    WsaEShutdown    (10058, "Cannot send after socket shutdown"),
    WsaETimedout    (10060, "Connection timed out"),
    WsaEConnRefused (10061, "Connection refused"),
    WsaEHostDown    (10064, "Host is down"),
    WsaEHostUnreach (10065, "No route to host"),
    WsaEProcLim     (10067, "Too many processes"),
    WsaSysNotReady  (10091, "Network subsystem is unavailable"),
    WsaVerNotSupp   (10092, "Winsock.dll version out of range"),
    WsaNotInit      (10093, "Successful WSAStartup not yet performed"),
    WsaEDiscon      (10101, "Graceful shutdown in progress"),
    WsaTypeNotFound (10109, "Class type not found"),
    WsaHostNotFound (11001, "Host not found"),
    WsaTryAgain     (11002, "Nonauthoritative host not found"),
    WsaNoRecovery   (11003, "This is a nonrecoverable error"),
    WsaNoData       (11004, "Valid name, no data record of requested type");

    private int code;
    private String message;

    private WinsockError(final int code, final String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public String toString() {
        return message;
    }
    
    public int getCode() {
        return code;
    }

    /**
     * Convert a Winsock error code to a text string
     *
     * @param sts
     *            int
     * @return String
     */
    public static final String asString(int sts) {
        for (WinsockError err : WinsockError.values()) {
            if (err.code == sts) {
                return err.message;
            }
        }
        return "Unknown Winsock error 0x" + Integer.toHexString(sts);
    }
}
