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

package org.alfresco.jlan.test.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.InputStream;
import java.io.OutputStream;

import static org.testng.Assert.*;

import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import jcifs.smb.SmbFile;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
/**
 * Open File With Shared Read Test Class
 *
 * @author gkspencer
 */
public class OpenFileShareReadIT extends ParameterizedJcifsTest {
    /**
     * Default constructor
     */
    public OpenFileShareReadIT() {
        super("OpenFileShareReadIT");
    }

    private void doTest(final int iteration) throws Exception {
        final String testFileName = getPerTestFileName(iteration);
        final SmbFile readWriteFile = new SmbFile(getRoot(), testFileName, SmbFile.FILE_SHARE_READ); 
        final NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(getRoot().getURL().getUserInfo());
        final SmbFile readOnlyFile = new SmbFile(getRoot().getURL().toString(), testFileName, auth, SmbFile.FILE_SHARE_READ);

        assertThat(readWriteFile.exists(), is(false));
        assertThat(readOnlyFile.exists(), is(false));
        readWriteFile.createNewFile();
        assertThat(readWriteFile.exists(), is(true));
        assertThat(readOnlyFile.exists(), is(true));

        try (OutputStream writeableStream = readWriteFile.getOutputStream()) {
            assertNotNull(writeableStream, "OutputStream");
            try (OutputStream nonWriteableStream = readOnlyFile.getOutputStream()) {
                fail("Should not be reachable because the file must be read only");
            } catch (SmbException ex) {
                int exceptionStatus = ex.getNtStatus(); 
                if (exceptionStatus != SmbException.NT_STATUS_ACCESS_DENIED ||
                        exceptionStatus != SmbException.NT_STATUS_ACCESS_VIOLATION) {
                    fail("Caught exception", ex);
                }
            }
            try (InputStream readOnlyStream = readOnlyFile.getInputStream()) {
                assertNotNull(readOnlyStream, "InputStream");
            }
        }
    }

    @Parameters({"iterations"})
    @Test(groups = "broken")
    public void test(@Optional("1") int iterations) throws Exception {
        for (int i = 0; i < iterations; i++) {
            doTest(i);
        }
    }
}