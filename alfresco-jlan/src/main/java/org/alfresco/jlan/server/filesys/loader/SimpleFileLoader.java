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

package org.alfresco.jlan.server.filesys.loader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import org.alfresco.jlan.server.SrvSession;
import org.alfresco.jlan.server.core.DeviceContext;
import org.alfresco.jlan.server.filesys.AccessDeniedException;
import org.alfresco.jlan.server.filesys.FileInfo;
import org.alfresco.jlan.server.filesys.FileName;
import org.alfresco.jlan.server.filesys.FileOpenParams;
import org.alfresco.jlan.server.filesys.NetworkFile;
import org.alfresco.jlan.server.filesys.db.DBDeviceContext;
import org.alfresco.jlan.server.filesys.db.LocalDataNetworkFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.extensions.config.ConfigElement;

/**
 * Simple File Loader Class
 *
 * <p>
 * The simple file loader class maps the file load/store requests to files within the local filesystem.
 *
 * @author gkspencer
 */
public class SimpleFileLoader implements FileLoader, NamedFileLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleFileLoader.class);

    // Local path that the virtual filesystem is mapped to
    private String m_rootPath;

    /**
     * Default constructor
     */
    public SimpleFileLoader() {
    }

    /**
     * Return the database features required by this file loader. Return zero if no database features are required by the loader.
     *
     * @return int
     */
    @Override
    public int getRequiredDBFeatures() {
        // No database features required
        return 0;
    }

    /**
     * Open/create a file
     *
     * @param params
     *            FileOpenParams
     * @param fid
     *            int
     * @param stid
     *            int
     * @param did
     *            int
     * @param create
     *            boolean
     * @param dir
     *            boolean
     * @exception IOException
     * @exception FileNotFoundException
     */
    @Override
    public NetworkFile openFile(final FileOpenParams params, final int fid, final int stid, final int did, final boolean create, final boolean dir)
            throws IOException, FileNotFoundException {
        // Get the full path for the new file
        String fullName = FileName.buildPath(getRootPath(), params.getPath(), null, java.io.File.separatorChar);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("SimpleFileLoader.openFile() fname=" + params.getPath() + ", fid=" + fid + ", did=" + did + ", fullName=" + fullName);
        }

        // Check if the file exists
        File file = new File(fullName);
        if (file.exists() == false) {
            // Try and map the file name string to a local path
            final String mappedPath = FileName.mapPath(getRootPath(), params.getPath());
            if (mappedPath == null && create == false) {
                throw new FileNotFoundException("File does not exist, " + params.getPath());
            }

            // Create the file object for the mapped file and check if the file exists
            file = new File(mappedPath);
            if (file.exists() == false && create == false) {
                throw new FileNotFoundException("File does not exist, " + params.getPath());
            }

            // Set the new full path
            fullName = mappedPath;
        }

        // Create the new file, if create is enabled
        if (create) {
            final FileWriter newFile = new FileWriter(file);
            newFile.close();
        }

        // Create a Java network file
        file = new File(fullName);
        final LocalDataNetworkFile netFile = new LocalDataNetworkFile(params.getPath(), fid, did, file);
        netFile.setGrantedAccess(NetworkFile.READWRITE);

        // Return the network file
        return netFile;
    }

    /**
     * Close the network file
     *
     * @param sess
     *            SrvSession
     * @param netFile
     *            NetworkFile
     * @exception IOException
     */
    @Override
    public void closeFile(final SrvSession sess, final NetworkFile netFile) throws IOException {
        // Close the file
        netFile.closeFile();
    }

    /**
     * Delete a file
     *
     * @param fname
     *            String
     * @param fid
     *            int
     * @param stid
     *            int
     * @exception IOException
     */
    @Override
    public void deleteFile(final String fname, final int fid, final int stid) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("SimpleFileLoader.deleteFile() fname=" + fname + ", fid=" + fid);
        }

        // Get the full path for the file
        final String name = FileName.buildPath(getRootPath(), fname, null, java.io.File.separatorChar);

        // Check if the file exists, and it is a file
        final File delFile = new File(name);
        if (delFile.exists() && delFile.isFile()) {
            // Delete the file
            delFile.delete();
        }
    }

    /**
     * Create a directory
     *
     * @param dir
     *            String
     * @param fid
     *            int
     * @exception IOException
     */
    @Override
    public void createDirectory(final String dir, final int fid) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("SimpleFileLoader.createDirectory() dir=" + dir + ", fid=" + fid);
        }

        // Get the full path for the new directory
        final String dirname = FileName.buildPath(getRootPath(), dir, null, java.io.File.separatorChar);

        // Create the new directory
        final File newDir = new File(dirname);
        newDir.mkdir();
    }

    /**
     * Delete a directory
     *
     * @param dir
     *            String
     * @param fid
     *            int
     * @exception IOException
     */
    @Override
    public void deleteDirectory(final String dir, final int fid) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("SimpleFileLoader.deleteDirectory() dir=" + dir + ", fid=" + fid);
        }

        // Get the full path for the directory
        final String dirname = FileName.buildPath(getRootPath(), dir, null, java.io.File.separatorChar);

        // Check if the directory exists, and it is a directory
        File delDir = new File(dirname);
        if (delDir.exists() && delDir.isDirectory()) {
            // Check if the directory contains any files
            final String[] fileList = delDir.list();
            if (fileList != null && fileList.length > 0) {
                throw new AccessDeniedException("Directory not empty");
            }

            // Delete the directory
            delDir.delete();
        }

        // If the path does not exist then try and map it to a real path, there may be case differences
        else if (delDir.exists() == false) {
            // Map the path to a real path
            final String mappedPath = FileName.mapPath(getRootPath(), dir);
            if (mappedPath != null) {
                // Check if the path is a directory
                delDir = new File(mappedPath);
                if (delDir.isDirectory()) {
                    // Check if the directory contains any files
                    final String[] fileList = delDir.list();
                    if (fileList != null && fileList.length > 0) {
                        throw new AccessDeniedException("Directory not empty");
                    }

                    // Delete the directory
                    delDir.delete();
                }
            }
        }
    }

    /**
     * Set file information
     *
     * @param path
     *            String
     * @param fid
     *            int
     * @param finfo
     *            FileInfo
     * @exception IOException
     */
    @Override
    public void setFileInformation(final String path, final int fid, final FileInfo finfo) throws IOException {
    }

    /**
     * Rename a file or directory
     *
     * @param curName
     *            String
     * @param fid
     *            int
     * @param newName
     *            String
     * @param isdir
     *            boolean
     * @exception IOException
     */
    @Override
    public void renameFileDirectory(final String curName, final int fid, final String newName, final boolean isdir) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("SimpleFileLoader.renameFileDirectory() curName=" + curName + ", fid=" + fid + ", newName=" + newName);
        }

        // Get the full path for the existing file and the new file name
        final String curPath = FileName.buildPath(getRootPath(), curName, null, java.io.File.separatorChar);
        final String newPath = FileName.buildPath(getRootPath(), newName, null, java.io.File.separatorChar);

        // Rename the file
        final File curFile = new File(curPath);
        final File newFile = new File(newPath);

        if (curFile.renameTo(newFile) == false) {
            throw new IOException("Rename " + curPath + " to " + newPath + " failed");
        }
    }

    /**
     * Return the root path
     *
     * @return String
     */
    protected final String getRootPath() {
        return m_rootPath;
    }

    /**
     * Initialize the file loader using the specified parameters
     *
     * @param params
     *            ConfigElement
     * @param ctx
     *            DeviceContext
     * @exception FileLoaderException
     */
    @Override
    public void initializeLoader(final ConfigElement params, final DeviceContext ctx) throws FileLoaderException {
        // Get the root path to be used to load/store files
        final ConfigElement nameVal = params.getChild("RootPath");
        if (nameVal == null || nameVal.getValue() == null || nameVal.getValue().length() == 0) {
            throw new FileLoaderException("SimpleFileLoader RootPath parameter required");
        }
        m_rootPath = nameVal.getValue();

        // Check that the root path is valid
        final File root = new File(m_rootPath);
        if (root.exists() == false || root.isFile()) {
            throw new FileLoaderException("SimpleFileLoader RootPath does not exist or is not a directory, " + m_rootPath);
        }
    }

    /**
     * Load/save file data, not implemented in this loader.
     *
     * @param fileReq
     *            FileRequest
     */
    @Override
    public final void queueFileRequest(final FileRequest fileReq) {
        // Nothing to do
    }

    /**
     * Start the file loader
     *
     * @param ctx
     *            DeviceContext
     */
    @Override
    public void startLoader(final DeviceContext ctx) {
        // Nothing to do
    }

    /**
     * Shutdown the file loader and release all resources
     *
     * @param immediate
     *            boolean
     */
    @Override
    public void shutdownLoader(final boolean immediate) {
        // Nothing to do
    }

    /**
     * Indicate that the file loader does not support NTFS streams
     *
     * @return boolean
     */
    @Override
    public boolean supportsStreams() {
        return false;
    }

    /**
     * Add a file processor to process files before storing and after loading.
     *
     * @param fileProc
     *            FileProcessor
     * @throws FileLoaderException
     */
    @Override
    public void addFileProcessor(final FileProcessor fileProc) throws FileLoaderException {
        // Not supported by this file loader implementation
        throw new FileLoaderException("File processors not supported");
    }

    /**
     * Set the database context
     *
     * @param dbCtx
     *            DBDeviceContext
     */
    @Override
    public final void setContext(final DBDeviceContext dbCtx) {
    }
}
