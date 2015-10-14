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

package org.alfresco.jlan.sample;

import org.alfresco.jlan.server.filesys.DiskDeviceContext;
import org.alfresco.jlan.server.filesys.cache.FileState;
import org.alfresco.jlan.server.filesys.loader.FileProcessor;
import org.alfresco.jlan.server.filesys.loader.FileSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test File Processor Class
 *
 * @author gkspencer
 */
public class TestFileProcessor implements FileProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestFileProcessor.class);

    /**
     * Process a cached file just before it is to be stored.
     *
     * @param context
     *            DiskDeviceContext
     * @param state
     *            FileState
     * @param segment
     *            FileSegment
     */
    @Override
    public void processStoredFile(final DiskDeviceContext context, final FileState state, final FileSegment segment) {
        try {
            LOGGER.info("## TestFileProcessor Storing file={}, fid={}, cache={}", state.getPath(), state.getFileId(), segment.getTemporaryFile());
        } catch (final Exception ex) {
        }
    }

    /**
     * Process a cached file just after being loaded.
     *
     * @param context
     *            DiskDeviceContext
     * @param state
     *            FileState
     * @param segment
     *            FileSegment
     */
    @Override
    public void processLoadedFile(final DiskDeviceContext context, final FileState state, final FileSegment segment) {
        try {
            LOGGER.info("## TestFileProcessor Loaded file={}, fid={}, cache={}", state.getPath(), state.getFileId(), segment.getTemporaryFile());
        } catch (final Exception ex) {
        }
    }

}
