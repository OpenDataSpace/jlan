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

package org.alfresco.jlan.server.filesys.db.mysql;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.alfresco.jlan.server.config.InvalidConfigurationException;
import org.alfresco.jlan.server.filesys.FileAttribute;
import org.alfresco.jlan.server.filesys.FileExistsException;
import org.alfresco.jlan.server.filesys.FileInfo;
import org.alfresco.jlan.server.filesys.FileName;
import org.alfresco.jlan.server.filesys.FileOpenParams;
import org.alfresco.jlan.server.filesys.FileStatus;
import org.alfresco.jlan.server.filesys.FileType;
import org.alfresco.jlan.server.filesys.cache.FileState;
import org.alfresco.jlan.server.filesys.db.DBDataDetails;
import org.alfresco.jlan.server.filesys.db.DBDataDetailsList;
import org.alfresco.jlan.server.filesys.db.DBDataInterface;
import org.alfresco.jlan.server.filesys.db.DBDeviceContext;
import org.alfresco.jlan.server.filesys.db.DBException;
import org.alfresco.jlan.server.filesys.db.DBFileInfo;
import org.alfresco.jlan.server.filesys.db.DBInterface;
import org.alfresco.jlan.server.filesys.db.DBObjectIdInterface;
import org.alfresco.jlan.server.filesys.db.DBQueueInterface;
import org.alfresco.jlan.server.filesys.db.DBSearchContext;
import org.alfresco.jlan.server.filesys.db.JdbcDBInterface;
import org.alfresco.jlan.server.filesys.db.ObjectIdFileLoader;
import org.alfresco.jlan.server.filesys.db.RetentionDetails;
import org.alfresco.jlan.server.filesys.loader.CachedFileInfo;
import org.alfresco.jlan.server.filesys.loader.FileRequest;
import org.alfresco.jlan.server.filesys.loader.FileRequestQueue;
import org.alfresco.jlan.server.filesys.loader.FileSegment;
import org.alfresco.jlan.server.filesys.loader.FileSegmentInfo;
import org.alfresco.jlan.server.filesys.loader.MultipleFileRequest;
import org.alfresco.jlan.server.filesys.loader.SingleFileRequest;
import org.alfresco.jlan.smb.server.ntfs.StreamInfo;
import org.alfresco.jlan.smb.server.ntfs.StreamInfoList;
import org.alfresco.jlan.util.MemorySize;
import org.alfresco.jlan.util.StringList;
import org.alfresco.jlan.util.WildCard;
import org.alfresco.jlan.util.db.DBConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.extensions.config.ConfigElement;

/**
 * mySQL Database Interface Class
 *
 * <p>
 * mySQL specific implementation of the database interface used by the database filesystem driver (DBDiskDriver).
 *
 * @author gkspencer
 */
public class MySQLDBInterface extends JdbcDBInterface implements DBQueueInterface, DBDataInterface, DBObjectIdInterface {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLDBInterface.class);

    // Memory buffer maximum size
    public final static long MaxMemoryBuffer = MemorySize.MEGABYTE / 2; // 1/2Mb

    // Lock file name, used to check if server shutdown was clean or not
    public final static String LockFileName = "MySQLLoader.lock";

    // MySQL error codes
    private static final int ErrorDuplicateEntry = 1062;

    // Database connection and prepared statement used to write file requests to the queue tables
    private Connection m_dbConn;
    private PreparedStatement m_reqStmt;
    private PreparedStatement m_tranStmt;

    /**
     * Default constructor
     */
    public MySQLDBInterface() {
        super();
    }

    /**
     * Return the database interface name
     *
     * @return String
     */
    @Override
    public String getDBInterfaceName() {
        return "mySQL";
    }

    /**
     * Get the supported database features mask
     *
     * @return int
     */
    @Override
    protected int getSupportedFeatures() {
        // Determine the available database interface features
        return FeatureNTFS + FeatureRetention + FeatureSymLinks + FeatureQueue + FeatureData + FeatureJarData + FeatureObjectId;
    }

    /**
     * Initialize the database interface
     *
     * @param dbCtx
     *            DBDeviceContext
     * @param params
     *            ConfigElement
     * @exception InvalidConfigurationException
     */
    @Override
    public void initializeDatabase(final DBDeviceContext dbCtx, final ConfigElement params) throws InvalidConfigurationException {
        // Set the JDBC driver class, must be set before the connection pool is created
        setDriverName("com.mysql.jdbc.Driver");

        // Call the base class to do the main initialization
        super.initializeDatabase(dbCtx, params);

        // Force the autoReconnect to be enabled
        if (getDSNString().indexOf("?autoReconnect=") == -1 && params.getChild("noAutoReconnect") == null) {
            setDSNString(getDSNString() + "?autoReconnect=true");
        }

        // Create the database connection pool
        try {
            createConnectionPool();
        } catch (final Exception ex) {
            if (hasDebug()) {
                LOGGER.error("[mySQL] Error creating connection pool, ", ex);
            }

            // Rethrow the exception
            throw new InvalidConfigurationException("Failed to create connection pool, " + ex.getMessage());
        }
        // Check if the file system table exists
        Connection conn = null;
        try {
            // Open a connection to the database
            conn = getConnection();

            final DatabaseMetaData dbMeta = conn.getMetaData();
            final ResultSet rs = dbMeta.getTables("", "", "", null);

            boolean foundStruct = false;
            boolean foundStream = false;
            boolean foundRetain = false;
            boolean foundQueue = false;
            boolean foundTrans = false;
            boolean foundData = false;
            boolean foundJarData = false;
            boolean foundObjId = false;
            boolean foundSymLink = false;

            while (rs.next()) {
                // Get the table name
                final String tblName = rs.getString("TABLE_NAME");

                // Check if we found the filesystem structure or streams table
                if (tblName.equalsIgnoreCase(getFileSysTableName())) {
                    foundStruct = true;
                } else if (hasStreamsTableName() && tblName.equalsIgnoreCase(getStreamsTableName())) {
                    foundStream = true;
                } else if (hasRetentionTableName() && tblName.equalsIgnoreCase(getRetentionTableName())) {
                    foundRetain = true;
                } else if (hasDataTableName() && tblName.equalsIgnoreCase(getDataTableName())) {
                    foundData = true;
                } else if (hasJarDataTableName() && tblName.equalsIgnoreCase(getJarDataTableName())) {
                    foundJarData = true;
                } else if (hasQueueTableName() && tblName.equalsIgnoreCase(getQueueTableName())) {
                    foundQueue = true;
                } else if (hasTransactionTableName() && tblName.equalsIgnoreCase(getTransactionTableName())) {
                    foundTrans = true;
                } else if (hasObjectIdTableName() && tblName.equalsIgnoreCase(getObjectIdTableName())) {
                    foundObjId = true;
                } else if (hasSymLinksTableName() && tblName.equalsIgnoreCase(getSymLinksTableName())) {
                    foundSymLink = true;
                }
            }

            // Check if the file system structure table should be created
            if (foundStruct == false) {
                // Create the file system structure table
                final Statement stmt = conn.createStatement();

                stmt.execute("CREATE TABLE " + getFileSysTableName()
                        + " (FileId INTEGER AUTO_INCREMENT, DirId INTEGER, FileName VARCHAR(255) BINARY NOT NULL, FileSize BIGINT,"
                        + "CreateDate BIGINT, ModifyDate BIGINT, AccessDate BIGINT, ChangeDate BIGINT, ReadOnly BIT, Archived BIT, Directory BIT,"
                        + "SystemFile BIT, Hidden BIT, IsSymLink BIT, Uid INTEGER, Gid INTEGER, Mode INTEGER, Deleted BIT NOT NULL DEFAULT 0, "
                        + "PRIMARY KEY (FileId));");

                // Create various indexes
                stmt.execute("ALTER TABLE " + getFileSysTableName() + " ADD UNIQUE INDEX IFileDirId (FileName,DirId);");
                stmt.execute("ALTER TABLE " + getFileSysTableName() + " ADD INDEX IDirId (DirId);");
                stmt.execute("ALTER TABLE " + getFileSysTableName() + " ADD INDEX IDir (DirId,Directory);");
                stmt.execute("ALTER TABLE " + getFileSysTableName() + " ADD UNIQUE INDEX IFileDirIdDir (FileName,DirId,Directory);");

                stmt.close();

                if (hasDebug()) {
                    LOGGER.info("[mySQL] Created table " + getFileSysTableName());
                }
            }

            // Check if the file streams table should be created
            if (isNTFSEnabled() && foundStream == false && getStreamsTableName() != null) {
                // Create the file streams table
                final Statement stmt = conn.createStatement();
                stmt.execute("CREATE TABLE " + getStreamsTableName()
                        + " (StreamId INTEGER AUTO_INCREMENT, FileId INTEGER NOT NULL, StreamName VARCHAR(255) BINARY NOT NULL, StreamSize BIGINT,"
                        + "CreateDate BIGINT, ModifyDate BIGINT, AccessDate BIGINT, PRIMARY KEY (StreamId));");

                // Create various indexes
                stmt.execute("ALTER TABLE " + getStreamsTableName() + " ADD INDEX IFileId (FileId);");

                stmt.close();

                if (hasDebug()) {
                    LOGGER.info("[mySQL] Created table " + getStreamsTableName());
                }
            }

            // Check if the retention table should be created
            if (isRetentionEnabled() && foundRetain == false && getRetentionTableName() != null) {
                // Create the retention period data table
                final Statement stmt = conn.createStatement();
                stmt.execute("CREATE TABLE " + getRetentionTableName() + " (FileId INTEGER NOT NULL, StartDate TIMESTAMP, EndDate TIMESTAMP,"
                        + "PurgeFlag TINYINT(1), PRIMARY KEY (FileId));");
                stmt.close();

                if (hasDebug()) {
                    LOGGER.info("[mySQL] Created table " + getRetentionTableName());
                }
            }

            // Check if the file loader queue table should be created

            if (isQueueEnabled() && foundQueue == false && getQueueTableName() != null) {

                // Create the request queue data table

                final Statement stmt = conn.createStatement();

                stmt.execute("CREATE TABLE " + getQueueTableName() + " (FileId INTEGER NOT NULL, StreamId INTEGER NOT NULL, ReqType SMALLINT,"
                        + "SeqNo INTEGER AUTO_INCREMENT, TempFile TEXT, VirtualPath TEXT, QueuedAt TIMESTAMP, Attribs VARCHAR(512), PRIMARY KEY (SeqNo));");
                stmt.execute("ALTER TABLE " + getQueueTableName() + " ADD INDEX IFileId (FileId);");
                stmt.execute("ALTER TABLE " + getQueueTableName() + " ADD INDEX IFileIdType (FileId, ReqType);");

                stmt.close();

                if (hasDebug()) {
                    LOGGER.info("[mySQL] Created table " + getQueueTableName());
                }
            }

            // Check if the file loader transaction queue table should be created

            if (isQueueEnabled() && foundTrans == false && getTransactionTableName() != null) {

                // Create the transaction request queue data table

                final Statement stmt = conn.createStatement();

                stmt.execute("CREATE TABLE " + getTransactionTableName() + " (FileId INTEGER NOT NULL, StreamId INTEGER NOT NULL,"
                        + "TranId INTEGER NOT NULL, ReqType SMALLINT, TempFile TEXT, VirtualPath TEXT, QueuedAt TIMESTAMP,"
                        + "Attribs VARCHAR(512), PRIMARY KEY (FileId,StreamId,TranId));");

                stmt.close();

                if (hasDebug()) {
                    LOGGER.info("[mySQL] Created table " + getTransactionTableName());
                }
            }

            // Check if the file data table should be created
            if (isDataEnabled() && foundData == false && hasDataTableName()) {
                // Create the file data table
                final Statement stmt = conn.createStatement();
                stmt.execute("CREATE TABLE " + getDataTableName()
                        + " (FileId INTEGER NOT NULL, StreamId INTEGER NOT NULL, FragNo INTEGER, FragLen INTEGER, Data LONGBLOB, JarFile BIT, JarId INTEGER);");

                stmt.execute("ALTER TABLE " + getDataTableName() + " ADD INDEX IFileStreamId (FileId,StreamId);");
                stmt.execute("ALTER TABLE " + getDataTableName() + " ADD INDEX IFileId (FileId);");
                stmt.execute("ALTER TABLE " + getDataTableName() + " ADD INDEX IFileIdFrag (FileId,FragNo);");

                stmt.close();

                if (hasDebug()) {
                    LOGGER.info("[mySQL] Created table " + getDataTableName());
                }
            }

            // Check if the Jar file data table should be created

            if (isJarDataEnabled() && foundJarData == false && hasJarDataTableName()) {

                // Create the Jar file data table

                final Statement stmt = conn.createStatement();

                stmt.execute("CREATE TABLE " + getJarDataTableName() + " (JarId INTEGER AUTO_INCREMENT, Data LONGBLOB, PRIMARY KEY (JarId));");

                stmt.close();

                // DEBUG

                if (hasDebug()) {
                    LOGGER.info("[mySQL] Created table " + getJarDataTableName());
                }
            }

            // Check if the file id/object id mapping table should be created

            if (isObjectIdEnabled() && foundObjId == false && hasObjectIdTableName()) {

                // Create the file id/object id mapping table

                final Statement stmt = conn.createStatement();

                stmt.execute("CREATE TABLE " + getObjectIdTableName()
                        + " (FileId INTEGER NOT NULL, StreamId INTEGER NOT NULL, ObjectId VARCHAR(128), PRIMARY KEY (FileId,StreamId))");

                stmt.close();

                if (hasDebug()) {
                    LOGGER.info("[mySQL] Created table " + getObjectIdTableName());
                }
            }

            // Check if the symbolic links table should be created

            if (isSymbolicLinksEnabled() && foundSymLink == false && hasSymLinksTableName()) {

                // Create the symbolic links table

                final Statement stmt = conn.createStatement();

                stmt.execute("CREATE TABLE " + getSymLinksTableName() + " (FileId INTEGER NOT NULL PRIMARY KEY, SymLink VARCHAR(8192))");

                stmt.close();

                if (hasDebug()) {
                    LOGGER.info("[mySQL] Created table " + getSymLinksTableName());
                }
            }
        } catch (final Exception ex) {
            LOGGER.error("Error: " + ex.getMessage(), ex);
        } finally {

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }
    }

    /**
     * Check if a file/folder exists
     *
     * @param dirId
     *            int
     * @param fname
     *            String
     * @return FileStatus.NotExist, FileStatus.FileExists or FileStatus.DirectoryExists
     * @throws DBException
     */
    @Override
    public int fileExists(final int dirId, final String fname) throws DBException {

        // Check if the file exists, and whether it is a file or folder

        int sts = FileStatus.NotExist;

        Connection conn = null;
        Statement stmt = null;

        try {

            // Get a connection to the database, create a statement for the database lookup

            conn = getConnection();
            stmt = conn.createStatement();

            final String sql = "SELECT FileName,Directory FROM " + getFileSysTableName() + " WHERE DirId = " + dirId + " AND FileName = '"
                    + checkNameForSpecialChars(fname) + "';";

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] File exists SQL: " + sql);
            }

            // Search for the file/folder

            final ResultSet rs = stmt.executeQuery(sql);

            // Check if a file record exists

            if (rs.next()) {

                // Check if the record is for a file or folder

                if (rs.getBoolean("Directory") == true) {
                    sts = FileStatus.DirectoryExists;
                } else {
                    sts = FileStatus.FileExists;
                }
            }

            // Close the result set

            rs.close();
        } catch (final Exception ex) {
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the status

        return sts;
    }

    /**
     * Create a file record for a new file or folder
     *
     * @param fname
     *            String
     * @param dirId
     *            int
     * @param params
     *            FileOpenParams
     * @param retain
     *            boolean
     * @return int
     * @exception DBException
     * @exception FileExistsException
     */
    @Override
    public int createFileRecord(final String fname, final int dirId, final FileOpenParams params, final boolean retain)
            throws DBException, FileExistsException {

        // Create a new file record for a file/folder and return a unique file id

        Connection conn = null;
        PreparedStatement pstmt = null;
        Statement stmt = null;

        int fileId = -1;
        boolean duplicateKey = false;

        try {

            // Get a database connection

            conn = getConnection();

            // Check if the file already exists in the database

            stmt = conn.createStatement();
            final String chkFileName = checkNameForSpecialChars(fname);

            final String qsql = "SELECT FileName,FileId FROM " + getFileSysTableName() + " WHERE FileName = '" + chkFileName + "' AND DirId = " + dirId;

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Create file SQL: {}", qsql);
            }

            // Check if the file/folder already exists

            final ResultSet rs = stmt.executeQuery(qsql);
            if (rs.next()) {

                // File record already exists, return the existing file id

                fileId = rs.getInt("FileId");
                LOGGER.info("File record already exists for " + fname + ", fileId=" + fileId);
                return fileId;
            }

            // Check if a file or folder record should be created
            final boolean dirRec = params.isDirectory();

            // Get a statement
            final long timeNow = System.currentTimeMillis();

            pstmt = conn.prepareStatement("INSERT INTO " + getFileSysTableName()
                    + "(FileName,CreateDate,ModifyDate,AccessDate,DirId,Directory,ReadOnly,Archived,SystemFile,Hidden,FileSize,Gid,Uid,Mode,IsSymLink)"
                    + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            pstmt.setString(1, chkFileName);
            pstmt.setLong(2, timeNow);
            pstmt.setLong(3, timeNow);
            pstmt.setLong(4, timeNow);
            pstmt.setInt(5, dirId);
            pstmt.setBoolean(6, dirRec);
            pstmt.setBoolean(7, FileAttribute.hasAttribute(params.getAttributes(), FileAttribute.ReadOnly));
            pstmt.setBoolean(8, FileAttribute.hasAttribute(params.getAttributes(), FileAttribute.Archive));
            pstmt.setBoolean(9, FileAttribute.hasAttribute(params.getAttributes(), FileAttribute.System));
            pstmt.setBoolean(10, FileAttribute.hasAttribute(params.getAttributes(), FileAttribute.Hidden));
            pstmt.setInt(11, 0);

            pstmt.setInt(12, params.hasGid() ? params.getGid() : 0);
            pstmt.setInt(13, params.hasUid() ? params.getUid() : 0);
            pstmt.setInt(14, params.hasMode() ? params.getMode() : 0);

            pstmt.setBoolean(15, params.isSymbolicLink());

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Create file SQL: {}", pstmt.toString());
            }

            // Create an entry for the new file
            if (pstmt.executeUpdate() > 0) {
                // Get the last insert id
                final ResultSet rs2 = stmt.executeQuery("SELECT LAST_INSERT_ID();");

                if (rs2.next()) {
                    fileId = rs2.getInt(1);
                }

                // Check if the returned file id is valid
                if (fileId == -1) {
                    throw new DBException("Failed to get file id for " + fname);
                }

                // If retention is enabled then create a retention record for the new file/folder
                if (retain == true && isRetentionEnabled()) {
                    // Create a retention record for the new file/directory
                    final Timestamp startDate = new Timestamp(System.currentTimeMillis());
                    final Timestamp endDate = new Timestamp(startDate.getTime() + getRetentionPeriod());

                    final String rSql = "INSERT INTO " + getRetentionTableName() + " (FileId,StartDate,EndDate) VALUES (" + fileId + ",'" + startDate.toString()
                            + "','" + endDate.toString() + "');";

                    if (hasSQLDebug()) {
                        LOGGER.info("[mySQL] Add retention record SQL: {}", rSql);
                    }

                    // Add the retention record for the file/folder
                    stmt.executeUpdate(rSql);
                }

                // Check if the new file is a symbolic link
                if (params.isSymbolicLink()) {
                    // Create the symbolic link record
                    final String symSql = "INSERT INTO " + getSymLinksTableName() + " (FileId, SymLink) VALUES (" + fileId + ",'" + params.getSymbolicLinkName()
                            + "');";
                    if (hasSQLDebug()) {
                        LOGGER.info("[mySQL] Create symbolic link SQL: " + symSql);
                    }

                    // Add the symbolic link record
                    stmt.executeUpdate(symSql);
                }

                if (hasDebug()) {
                    LOGGER.info("[mySQL] Created file name=" + fname + ", dirId=" + dirId + ", fileId=" + fileId);
                }
            }
        } catch (final SQLException ex) {
            // Check for a duplicate key error, another client may have created the file
            if (ex.getErrorCode() == ErrorDuplicateEntry) {
                // Flag that a duplicate key error occurred, we can return the previously allocated file id
                duplicateKey = true;
            } else {
                // Rethrow the exception
                throw new DBException(ex.toString());
            }
        } catch (final Exception ex) {
            if (hasDebug()) {
                LOGGER.error("[mySQL] Create file record error {}", ex.getMessage());
            }

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Close the prepared statement
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (final Exception ex) {
                }
            }

            // Close the query statement
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // If a duplicate key error occurred get the previously allocated file id
        if (duplicateKey == true) {
            // Get the previously allocated file id for the file record
            fileId = getFileId(dirId, fname, false, true);
            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Duplicate key error, lookup file id, dirId={}, fname={}, fid={}", dirId, fname, fileId);
            }
        }

        // Return the allocated file id
        return fileId;
    }

    /**
     * Create a stream record for a new file stream
     *
     * @param sname
     *            String
     * @param fid
     *            int
     * @return int
     * @exception DBException
     */
    @Override
    public int createStreamRecord(final String sname, final int fid) throws DBException {
        // Create a new file stream attached to the specified file
        Connection conn = null;
        PreparedStatement stmt = null;
        Statement stmt2 = null;

        int streamId = -1;

        try {
            // Get a database connection
            conn = getConnection();

            // Get a statement
            final long timeNow = System.currentTimeMillis();

            stmt = conn.prepareStatement(
                    "INSERT INTO " + getStreamsTableName() + "(FileId,StreamName,CreateDate,ModifyDate,AccessDate,StreamSize) VALUES (?,?,?,?,?,?)");
            stmt.setInt(1, fid);
            stmt.setString(2, sname);
            stmt.setLong(3, timeNow);
            stmt.setLong(4, timeNow);
            stmt.setLong(5, timeNow);
            stmt.setInt(6, 0);

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Create stream SQL: {}", stmt);
            }

            // Create an entry for the new stream

            if (stmt.executeUpdate() > 0) {

                // Get the stream id for the newly created stream

                stmt2 = conn.createStatement();
                final ResultSet rs2 = stmt2.executeQuery("SELECT LAST_INSERT_ID();");

                if (rs2.next()) {
                    streamId = rs2.getInt(1);
                }
                rs2.close();
            }
        } catch (final Exception ex) {

            if (hasDebug()) {
                LOGGER.error("[mySQL] Create file stream error " + ex.getMessage());
            }

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Close the statements
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            if (stmt2 != null) {
                try {
                    stmt2.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the allocated stream id
        return streamId;
    }

    /**
     * Delete a file or folder record
     *
     * @param dirId
     *            int
     * @param fid
     *            int
     * @param markOnly
     *            boolean
     * @exception DBException
     */
    @Override
    public void deleteFileRecord(final int dirId, final int fid, final boolean markOnly) throws DBException {
        // Delete a file record from the database, or mark the file record as deleted
        Connection conn = null;
        Statement stmt = null;
        try {
            // Get a connection to the database
            conn = getConnection();

            // Delete the file entry from the database
            stmt = conn.createStatement();
            String sql = null;

            if (markOnly == true) {
                sql = "UPDATE " + getFileSysTableName() + " SET Deleted = 1 WHERE FileId = " + fid;
            } else {
                sql = "DELETE FROM " + getFileSysTableName() + " WHERE FileId = " + fid;
            }

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Delete file SQL: {}", sql);
            }

            // Delete the file/folder, or mark as deleted
            final int recCnt = stmt.executeUpdate(sql);
            if (recCnt == 0) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM " + getFileSysTableName() + " WHERE FileId = " + fid);
                while (rs.next()) {
                    LOGGER.info("Found file {}", rs.getString("FileName"));
                }

                throw new DBException("Failed to delete file record for fid=" + fid);
            }

            // Check if retention is enabled
            if (isRetentionEnabled()) {
                // Delete the retention record for the file
                sql = "DELETE FROM " + getRetentionTableName() + " WHERE FileId = " + fid;

                if (hasSQLDebug()) {
                    LOGGER.info("[mySQL] Delete retention SQL: {}", sql);
                }

                // Delete the file/folder retention record
                stmt.executeUpdate(sql);
            }
        } catch (final SQLException ex) {
            if (hasDebug()) {
                LOGGER.error("[mySQL] Delete file error {}", ex.getMessage());
            }

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Close the statement
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
            }
        }
    }

    /**
     * Delete a file stream record
     *
     * @param fid
     *            int
     * @param stid
     *            int
     * @param markOnly
     *            boolean
     * @exception DBException
     */
    @Override
    public void deleteStreamRecord(final int fid, final int stid, final boolean markOnly) throws DBException {
        // Delete a file stream from the database, or mark the stream as deleted
        Connection conn = null;
        Statement stmt = null;
        try {
            // Get a database connection
            conn = getConnection();
            // Get a statement
            stmt = conn.createStatement();
            final String sql = "DELETE FROM " + getStreamsTableName() + " WHERE FileId = " + fid + " AND StreamId = " + stid;

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Delete stream SQL: " + sql);
            }

            // Delete the stream record
            stmt.executeUpdate(sql);
        } catch (final Exception ex) {
            if (hasDebug()) {
                LOGGER.error("[mySQL] Delete stream error: {}", ex.getMessage());
            }

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }
    }

    /**
     * Set file information for a file or folder
     *
     * @param dirId
     *            int
     * @param fid
     *            int
     * @param finfo
     *            FileInfo
     * @exception DBException
     */
    @Override
    public void setFileInformation(final int dirId, final int fid, final FileInfo finfo) throws DBException {

        // Set file information fields

        Connection conn = null;
        Statement stmt = null;

        try {

            // Get a connection to the database

            conn = getConnection();

            // Build the SQL statement to update the file information settings

            final StringBuffer sql = new StringBuffer(256);
            sql.append("UPDATE ");
            sql.append(getFileSysTableName());
            sql.append(" SET ");

            // Check if the file attributes have been updated

            if (finfo.hasSetFlag(FileInfo.SetAttributes)) {

                // Update the basic file attributes

                sql.append("ReadOnly = ");
                sql.append(finfo.isReadOnly() ? "1" : "0");

                sql.append(", Archived =");
                sql.append(finfo.isArchived() ? "1" : "0");

                sql.append(", SystemFile = ");
                sql.append(finfo.isSystem() ? "1" : "0");

                sql.append(", Hidden = ");
                sql.append(finfo.isHidden() ? "1" : "0");
                sql.append(",");
            }

            // Check if the file size should be set

            if (finfo.hasSetFlag(FileInfo.SetFileSize)) {

                // Update the file size

                sql.append("FileSize = ");
                sql.append(finfo.getSize());
                sql.append(",");
            }

            // Merge the group id, user id and mode into the in-memory file information

            if (finfo.hasSetFlag(FileInfo.SetGid)) {

                // Update the group id

                sql.append("Gid = ");
                sql.append(finfo.getGid());
                sql.append(",");
            }

            if (finfo.hasSetFlag(FileInfo.SetUid)) {

                // Update the user id

                sql.append("Uid = ");
                sql.append(finfo.getUid());
                sql.append(",");
            }

            if (finfo.hasSetFlag(FileInfo.SetMode)) {

                // Update the mode

                sql.append("Mode = ");
                sql.append(finfo.getMode());
                sql.append(",");
            }

            // Check if the access date/time has been set

            if (finfo.hasSetFlag(FileInfo.SetAccessDate)) {

                // Add the SQL to update the access date/time

                sql.append(" AccessDate = ");
                sql.append(finfo.getAccessDateTime());
                sql.append(",");
            }

            // Check if the modify date/time has been set

            if (finfo.hasSetFlag(FileInfo.SetModifyDate)) {

                // Add the SQL to update the modify date/time

                sql.append(" ModifyDate = ");
                sql.append(finfo.getModifyDateTime());
                sql.append(",");
            }

            // Check if the inode change date/time has been set

            if (finfo.hasSetFlag(FileInfo.SetChangeDate)) {

                // Add the SQL to update the change date/time

                sql.append(" ChangeDate = ");
                sql.append(finfo.getChangeDateTime());
            }

            // Trim any trailing comma

            if (sql.charAt(sql.length() - 1) == ',') {
                sql.setLength(sql.length() - 1);
            }

            // Complete the SQL request string
            sql.append(" WHERE FileId = ");
            sql.append(fid);
            sql.append(";");

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Set file info SQL: {}", sql);
            }

            // Create the SQL statement
            stmt = conn.createStatement();
            stmt.executeUpdate(sql.toString());
        } catch (final SQLException ex) {
            if (hasDebug()) {
                LOGGER.error("[mySQL] Set file information error {}", ex.getMessage());
            }

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Close the statement
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
            }
        }
    }

    /**
     * Set information for a file stream
     *
     * @param dirId
     *            int
     * @param fid
     *            int
     * @param stid
     *            int
     * @param sinfo
     *            StreamInfo
     * @exception DBException
     */
    @Override
    public void setStreamInformation(final int dirId, final int fid, final int stid, final StreamInfo sinfo) throws DBException {
        // Set file stream information fields
        Connection conn = null;
        Statement stmt = null;
        try {
            // Get a connection to the database
            conn = getConnection();

            // Build the SQL statement to update the file information settings
            final StringBuffer sql = new StringBuffer(256);
            sql.append("UPDATE ");
            sql.append(getStreamsTableName());
            sql.append(" SET ");

            // Check if the access date/time has been set
            if (sinfo.hasSetFlag(StreamInfo.SetAccessDate)) {
                // Add the SQL to update the access date/time
                sql.append(" AccessDate = ");
                sql.append(sinfo.getAccessDateTime());
                sql.append(",");
            }

            // Check if the modify date/time has been set
            if (sinfo.hasSetFlag(StreamInfo.SetModifyDate)) {
                // Add the SQL to update the modify date/time
                sql.append(" ModifyDate = ");
                sql.append(sinfo.getModifyDateTime());
                sql.append(",");
            }

            // Check if the stream size should be updated
            if (sinfo.hasSetFlag(StreamInfo.SetStreamSize)) {
                // Update the stream size
                sql.append(" StreamSize = ");
                sql.append(sinfo.getSize());
            }

            // Trim any trailing comma
            if (sql.charAt(sql.length() - 1) == ',') {
                sql.setLength(sql.length() - 1);
            }

            // Complete the SQL request string
            sql.append(" WHERE FileId = ");
            sql.append(fid);
            sql.append(" AND StreamId = ");
            sql.append(stid);
            sql.append(";");

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Set stream info SQL: {}", sql);
            }

            // Create the SQL statement

            stmt = conn.createStatement();
            stmt.executeUpdate(sql.toString());
        } catch (final SQLException ex) {
            if (hasDebug()) {
                LOGGER.error("[mySQL] Set stream information error {}", ex.getMessage());
            }

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Close the statement
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
            }
        }
    }

    /**
     * Get the id for a file/folder, or -1 if the file/folder does not exist.
     *
     * @param dirId
     *            int
     * @param fname
     *            String
     * @param dirOnly
     *            boolean
     * @param caseLess
     *            boolean
     * @return int
     * @throws DBException
     */
    @Override
    public int getFileId(final int dirId, final String fname, final boolean dirOnly, final boolean caseLess) throws DBException {
        // Get the file id for a file/folder
        int fileId = -1;

        Connection conn = null;
        Statement stmt = null;

        try {
            // Get a connection to the database, create a statement for the database lookup
            conn = getConnection();
            stmt = conn.createStatement();

            // Build the SQL for the file lookup
            final StringBuffer sql = new StringBuffer(128);

            sql.append("SELECT FileId FROM ");
            sql.append(getFileSysTableName());
            sql.append(" WHERE DirId = ");
            sql.append(dirId);
            sql.append(" AND ");

            // Check if the search is for a directory only
            if (dirOnly == true) {
                // Search for a directory record
                sql.append(" Directory = 1 AND ");
            }

            // Check if the file name search should be caseless
            if (caseLess == true) {
                // Perform a caseless search
                sql.append(" UPPER(FileName) = '");
                sql.append(checkNameForSpecialChars(fname).toUpperCase());
                sql.append("';");
            } else {
                // Perform a case sensitive search
                sql.append(" FileName = '");
                sql.append(checkNameForSpecialChars(fname));
                sql.append("';");
            }

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Get file id SQL: {}", sql);
            }

            // Run the database search
            final ResultSet rs = stmt.executeQuery(sql.toString());

            // Check if a file record exists
            if (rs.next()) {
                // Get the unique file id for the file or folder
                fileId = rs.getInt("FileId");
            }

            // Close the result set
            rs.close();
        } catch (final Exception ex) {
            if (hasDebug()) {
                LOGGER.error("[mySQL] Get file id error {}", ex.getMessage());
            }

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the file id, or -1 if not found

        return fileId;
    }

    /**
     * Get information for a file or folder
     *
     * @param dirId
     *            int
     * @param fid
     *            int
     * @param infoLevel
     *            int
     * @return FileInfo
     * @exception DBException
     */
    @Override
    public DBFileInfo getFileInformation(final int dirId, final int fid, final int infoLevel) throws DBException {

        // Create a SQL select for the required file information

        final StringBuffer sql = new StringBuffer(128);

        sql.append("SELECT ");

        // Select fields according to the required information level

        switch (infoLevel) {

            // File name only

            case DBInterface.FileNameOnly:
                sql.append("FileName");
                break;

            // File ids and name

            case DBInterface.FileIds:
                sql.append("FileName,FileId,DirId");
                break;

            // All file information

            case DBInterface.FileAll:
                sql.append("*");
                break;

            // Unknown information level

            default:
                throw new DBException("Invalid information level, " + infoLevel);
        }

        sql.append(" FROM ");
        sql.append(getFileSysTableName());
        sql.append(" WHERE FileId = ");
        sql.append(fid);

        if (hasSQLDebug()) {
            LOGGER.info("[mySQL] Get file info SQL: {}", sql);
        }

        // Load the file record

        Connection conn = null;
        Statement stmt = null;

        DBFileInfo finfo = null;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            // Load the file record

            final ResultSet rs = stmt.executeQuery(sql.toString());

            if (rs != null && rs.next()) {

                // Create the file informaiton object

                finfo = new DBFileInfo();
                finfo.setFileId(fid);

                // Load the file information

                switch (infoLevel) {

                    // File name only

                    case DBInterface.FileNameOnly:
                        finfo.setFileName(rs.getString("FileName"));
                        break;

                    // File ids and name

                    case DBInterface.FileIds:
                        finfo.setFileName(rs.getString("FileName"));
                        finfo.setDirectoryId(rs.getInt("DirId"));
                        break;

                    // All file information

                    case DBInterface.FileAll:
                        finfo.setFileName(rs.getString("FileName"));
                        finfo.setSize(rs.getLong("FileSize"));
                        finfo.setAllocationSize(finfo.getSize());
                        finfo.setDirectoryId(rs.getInt("DirId"));

                        // Load the various file date/times

                        finfo.setCreationDateTime(rs.getLong("CreateDate"));
                        finfo.setModifyDateTime(rs.getLong("ModifyDate"));
                        finfo.setAccessDateTime(rs.getLong("AccessDate"));
                        finfo.setChangeDateTime(rs.getLong("ChangeDate"));

                        // Build the file attributes flags

                        int attr = 0;

                        if (rs.getBoolean("ReadOnly") == true) {
                            attr += FileAttribute.ReadOnly;
                        }

                        if (rs.getBoolean("SystemFile") == true) {
                            attr += FileAttribute.System;
                        }

                        if (rs.getBoolean("Hidden") == true) {
                            attr += FileAttribute.Hidden;
                        }

                        if (rs.getBoolean("Directory") == true) {
                            attr += FileAttribute.Directory;
                            finfo.setFileType(FileType.Directory);
                        } else {
                            finfo.setFileType(FileType.RegularFile);
                        }

                        if (rs.getBoolean("Archived") == true) {
                            attr += FileAttribute.Archive;
                        }

                        finfo.setFileAttributes(attr);

                        // Get the group/owner id

                        finfo.setGid(rs.getInt("Gid"));
                        finfo.setUid(rs.getInt("Uid"));

                        finfo.setMode(rs.getInt("Mode"));

                        // Check if the file is a symbolic link

                        if (rs.getBoolean("IsSymLink") == true) {
                            finfo.setFileType(FileType.SymbolicLink);
                        }
                        break;
                }
            }
        } catch (final Exception ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error("[mySQL] Get file information error {}", ex.getMessage());
            }

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the file information

        return finfo;
    }

    /**
     * Get information for a file stream
     *
     * @param fid
     *            int
     * @param stid
     *            int
     * @param infoLevel
     *            int
     * @return StreamInfo
     * @exception DBException
     */
    @Override
    public StreamInfo getStreamInformation(final int fid, final int stid, final int infoLevel) throws DBException {

        // Create a SQL select for the required stream information

        final StringBuffer sql = new StringBuffer(128);

        sql.append("SELECT ");

        // Select fields according to the required information level

        switch (infoLevel) {

            // Stream name only.
            //
            // Also used if ids are requested as we already have the ids

            case DBInterface.StreamNameOnly:
            case DBInterface.StreamIds:
                sql.append("StreamName");
                break;

            // All file information

            case DBInterface.StreamAll:
                sql.append("*");
                break;

            // Unknown information level

            default:
                throw new DBException("Invalid information level, " + infoLevel);
        }

        sql.append(" FROM ");
        sql.append(getStreamsTableName());
        sql.append(" WHERE FileId = ");
        sql.append(fid);
        sql.append(" AND StreamId = ");
        sql.append(stid);

        if (hasSQLDebug()) {
            LOGGER.info("[mySQL] Get stream info SQL: {}", sql);
        }

        // Load the stream record
        Connection conn = null;
        Statement stmt = null;

        StreamInfo sinfo = null;
        try {
            // Get a connection to the database
            conn = getConnection();
            stmt = conn.createStatement();

            // Load the stream record
            final ResultSet rs = stmt.executeQuery(sql.toString());

            if (rs != null && rs.next()) {

                // Create the stream informaiton object

                sinfo = new StreamInfo("", fid, stid);

                // Load the file information

                switch (infoLevel) {

                    // Stream name only (or name and ids)

                    case DBInterface.StreamNameOnly:
                    case DBInterface.StreamIds:
                        sinfo.setName(rs.getString("StreamName"));
                        break;

                    // All stream information

                    case DBInterface.FileAll:
                        sinfo.setName(rs.getString("StreamName"));
                        sinfo.setSize(rs.getLong("StreamSize"));

                        // Load the various file date/times

                        sinfo.setCreationDateTime(rs.getLong("CreateDate"));
                        sinfo.setModifyDateTime(rs.getLong("ModifyDate"));
                        sinfo.setAccessDateTime(rs.getLong("AccessDate"));
                        break;
                }
            }
        } catch (final Exception ex) {
            if (hasDebug()) {
                LOGGER.error("[mySQL] Get stream information error {}", ex.getMessage());
            }

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Close the statement
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the stream information
        return sinfo;
    }

    /**
     * Return the list of streams for the specified file
     *
     * @param fid
     *            int
     * @param infoLevel
     *            int
     * @return StreamInfoList
     * @exception DBException
     */
    @Override
    public StreamInfoList getStreamsList(final int fid, final int infoLevel) throws DBException {
        // Create a SQL select for the required stream information
        final StringBuffer sql = new StringBuffer(128);
        sql.append("SELECT ");

        // Select fields according to the required information level
        switch (infoLevel) {
            // Stream name only.
            case DBInterface.StreamNameOnly:
                sql.append("StreamName");
                break;

            // Stream name and ids
            case DBInterface.StreamIds:
                sql.append("StreamName,FileId,StreamId");
                break;

            // All file information
            case DBInterface.StreamAll:
                sql.append("*");
                break;

            // Unknown information level
            default:
                throw new DBException("Invalid information level, " + infoLevel);
        }

        sql.append(" FROM ");
        sql.append(getStreamsTableName());
        sql.append(" WHERE FileId = ");
        sql.append(fid);

        // DEBUG

        if (hasSQLDebug()) {
            LOGGER.info("[mySQL] Get stream list SQL: {}", sql);
        }

        // Load the stream record

        Connection conn = null;
        Statement stmt = null;

        StreamInfoList sList = null;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            // Load the stream records

            final ResultSet rs = stmt.executeQuery(sql.toString());
            sList = new StreamInfoList();

            while (rs.next()) {

                // Create the stream informaiton object

                final StreamInfo sinfo = new StreamInfo("", fid, -1);

                // Load the file information

                switch (infoLevel) {

                    // Stream name only

                    case DBInterface.StreamNameOnly:
                        sinfo.setName(rs.getString("StreamName"));
                        break;

                    // Stream name and id

                    case DBInterface.StreamIds:
                        sinfo.setName(rs.getString("StreamName"));
                        sinfo.setStreamId(rs.getInt("StreamId"));
                        break;

                    // All stream information

                    case DBInterface.FileAll:
                        sinfo.setName(rs.getString("StreamName"));
                        sinfo.setStreamId(rs.getInt("StreamId"));
                        sinfo.setSize(rs.getLong("StreamSize"));

                        // Load the various file date/times

                        sinfo.setCreationDateTime(rs.getLong("CreateDate"));
                        sinfo.setModifyDateTime(rs.getLong("ModifyDate"));
                        sinfo.setAccessDateTime(rs.getLong("AccessDate"));
                        break;
                }

                // Add the stream information to the list

                sList.addStream(sinfo);
            }
        } catch (final Exception ex) {
            if (hasDebug()) {
                LOGGER.error("[mySQL] Get stream list error {}", ex.getMessage());
            }

            // Rethrow the exception

            throw new DBException(ex.toString());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the streams list

        return sList;
    }

    /**
     * Rename a file or folder, may also change the parent directory.
     *
     * @param dirId
     *            int
     * @param fid
     *            int
     * @param newName
     *            String
     * @param newDir
     *            int
     * @exception DBException
     * @exception FileNotFoundException
     */
    @Override
    public void renameFileRecord(final int dirId, final int fid, final String newName, final int newDir) throws DBException, FileNotFoundException {

        // Rename a file/folder

        Connection conn = null;
        Statement stmt = null;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            // Update the file record

            stmt = conn.createStatement();
            final String sql = "UPDATE " + getFileSysTableName() + " SET FileName = '" + checkNameForSpecialChars(newName) + "', DirId = " + newDir
                    + ", ChangeDate = " + System.currentTimeMillis() + " WHERE FileId = " + fid;

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Rename SQL: " + sql.toString());
            }

            // Rename the file/folder

            if (stmt.executeUpdate(sql) == 0) {

                // Original file not found

                throw new FileNotFoundException("" + fid);
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error("[mySQL] Rename file error " + ex.getMessage());
            }

            // Rethrow the exception

            throw new DBException(ex.toString());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }
    }

    /**
     * Rename a file stream
     *
     * @param dirId
     *            int
     * @param fid
     *            int
     * @param stid
     *            int
     * @param newName
     *            String
     * @exception DBException
     */
    @Override
    public void renameStreamRecord(final int dirId, final int fid, final int stid, final String newName) throws DBException {
        // TODO Auto-generated method stub

    }

    /**
     * Return the retention period expiry date/time for the specified file, or zero if the file/folder is not under retention.
     *
     * @param dirId
     *            int
     * @param fid
     *            int
     * @return RetentionDetails
     * @exception DBException
     */
    @Override
    public RetentionDetails getFileRetentionDetails(final int dirId, final int fid) throws DBException {

        // Check if retention is enabled

        if (isRetentionEnabled() == false) {
            return null;
        }

        // Get the retention record for the file/folder

        Connection conn = null;
        Statement stmt = null;

        RetentionDetails retDetails = null;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            // Get the retention record, if any

            retDetails = getRetentionExpiryDateTime(conn, stmt, fid);
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error("[mySQL] Get retention error " + ex.getMessage());
            }

            // Rethrow the exception

            throw new DBException(ex.toString());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the retention expiry date/time

        return retDetails;
    }

    /**
     * Start a directory search
     *
     * @param dirId
     *            int
     * @param searchPath
     *            String
     * @param attrib
     *            int
     * @param infoLevel
     *            int
     * @param maxRecords
     *            int
     * @return DBSearchContext
     * @exception DBException
     */
    @Override
    public DBSearchContext startSearch(final int dirId, final String searchPath, final int attrib, final int infoLevel, final int maxRecords)
            throws DBException {

        // Search for files/folders in the specified folder

        final StringBuffer sql = new StringBuffer(128);
        sql.append("SELECT * FROM ");
        sql.append(getFileSysTableName());

        sql.append(" WHERE DirId = ");
        sql.append(dirId);
        sql.append(" AND Deleted = 0");

        // Split the search path

        final String[] paths = FileName.splitPath(searchPath);

        // Check if the file name contains wildcard characters

        WildCard wildCard = null;

        if (WildCard.containsWildcards(searchPath)) {

            // For the '*.*' and '*' wildcards the SELECT will already return all files/directories
            // that are attached to the
            // parent directory. For 'name.*' and '*.ext' type wildcards we can use the LIKE clause
            // to filter the required
            // records, for more complex wildcards we will post-process the search using the
            // WildCard class to match the
            // file names.

            if (searchPath.endsWith("\\*.*") == false && searchPath.endsWith("\\*") == false) {

                // Create a wildcard search pattern

                wildCard = new WildCard(paths[1], true);

                // Check for a 'name.*' type wildcard

                if (wildCard.isType() == WildCard.WILDCARD_EXT) {

                    // Add the wildcard file extension selection clause to the SELECT

                    sql.append(" AND FileName LIKE('");
                    sql.append(checkNameForSpecialChars(wildCard.getMatchPart()));
                    sql.append("%')");

                    // Clear the wildcard object, we do not want it to filter the search results

                    wildCard = null;
                } else if (wildCard.isType() == WildCard.WILDCARD_NAME) {

                    // Add the wildcard file name selection clause to the SELECT

                    sql.append(" AND FileName LIKE('%");
                    sql.append(checkNameForSpecialChars(wildCard.getMatchPart()));
                    sql.append("')");

                    // Clear the wildcard object, we do not want it to filter the search results

                    wildCard = null;
                }
            }
        } else {

            // Search for a specific file/directory

            sql.append(" AND FileName = '");
            sql.append(checkNameForSpecialChars(paths[1]));
            sql.append("'");
        }

        // Return directories first

        sql.append(" ORDER BY Directory DESC");

        // Start the search

        ResultSet rs = null;
        Connection conn = null;
        Statement stmt = null;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Start search SQL: " + sql.toString());
            }

            // Start the folder search

            rs = stmt.executeQuery(sql.toString());
        } catch (final Exception ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error("[mySQL] Start search error " + ex.getMessage());
            }

            // Rethrow the exception

            throw new DBException(ex.toString());
        } finally {

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Create the search context, and return

        return new MySQLSearchContext(rs, wildCard);
    }

    /**
     * Return the used file space, or -1 if not supported.
     *
     * @return long
     */
    @Override
    public long getUsedFileSpace() {

        // Calculate the total used file space

        Connection conn = null;
        Statement stmt = null;

        long usedSpace = -1L;

        try {

            // Get a database connection and statement

            conn = getConnection();
            stmt = conn.createStatement();

            final String sql = "SELECT SUM(CAST(FileSize as BIGINT)) FROM " + getFileSysTableName();

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Get filespace SQL: " + sql);
            }

            // Calculate the currently used disk space

            final ResultSet rs = stmt.executeQuery(sql);

            if (rs.next()) {
                usedSpace = rs.getLong(1);
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error("[mySQL] Get used file space error " + ex.getMessage());
            }
        } finally {

            // Close the prepared statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the used file space

        return usedSpace;
    }

    /**
     * Queue a file request.
     *
     * @param req
     *            FileRequest
     * @exception DBException
     */
    @Override
    public void queueFileRequest(final FileRequest req) throws DBException {

        // Make sure the associated file state stays in memory for a short time, if the queue is
        // small
        // the request may get processed soon.

        if (req instanceof SingleFileRequest) {

            // Get the request details

            final SingleFileRequest fileReq = (SingleFileRequest) req;

            try {

                // Check if the file request queue database connection is valid

                if (m_dbConn == null || m_dbConn.isClosed() || m_reqStmt == null || m_reqStmt.getConnection().isClosed()) {
                    createQueueStatements();
                }

                // Check if the request is part of a transaction, or a standalone request

                if (fileReq.isTransaction() == false) {

                    // Write the file request record

                    int recCnt = 0;

                    synchronized (m_reqStmt) {

                        // Write the file request to the queue database

                        m_reqStmt.clearParameters();

                        m_reqStmt.setInt(1, fileReq.getFileId());
                        m_reqStmt.setInt(2, fileReq.getStreamId());
                        m_reqStmt.setInt(3, fileReq.isType());
                        m_reqStmt.setString(4, fileReq.getTemporaryFile());
                        m_reqStmt.setString(5, fileReq.getVirtualPath());
                        m_reqStmt.setString(6, fileReq.getAttributesString());

                        recCnt = m_reqStmt.executeUpdate();

                        // Retrieve the allocated sequence number

                        if (recCnt > 0) {

                            // Get the last insert id

                            final ResultSet rs2 = m_reqStmt.executeQuery("SELECT LAST_INSERT_ID();");

                            if (rs2.next()) {
                                fileReq.setSequenceNumber(rs2.getInt(1));
                            }
                        }
                    }
                } else {

                    // Check if the transaction prepared statement is valid, we may have lost the
                    // connection to the
                    // database.

                    if (m_tranStmt == null || m_tranStmt.getConnection().isClosed()) {
                        createQueueStatements();
                    }

                    // Write the transaction file request to the database

                    synchronized (m_tranStmt) {

                        // Write the request record to the database

                        m_tranStmt.clearParameters();

                        m_tranStmt.setInt(1, fileReq.getFileId());
                        m_tranStmt.setInt(2, fileReq.getStreamId());
                        m_tranStmt.setInt(3, fileReq.isType());
                        m_tranStmt.setInt(4, fileReq.getTransactionId());
                        m_tranStmt.setString(5, fileReq.getTemporaryFile());
                        m_tranStmt.setString(6, fileReq.getVirtualPath());
                        m_tranStmt.setString(7, fileReq.getAttributesString());

                        m_tranStmt.executeUpdate();
                    }
                }

                // File request was queued successfully, check for any offline file requests

                if (hasOfflineFileRequests()) {
                    databaseOnlineStatus(true);
                }
            } catch (final SQLException ex) {

                // If the request is a save then add to a pending queue to retry when the database
                // is back online

                if (fileReq.isType() == FileRequest.SAVE || fileReq.isType() == FileRequest.TRANSSAVE) {
                    queueOfflineSaveRequest(fileReq);
                }

                // DEBUG

                if (hasDebug()) {
                    LOGGER.error(ex.getMessage(), ex);
                }

                // Rethrow the exception

                throw new DBException(ex.getMessage());
            }
        }
    }

    /**
     * Perform a queue cleanup deleting temporary cache files that do not have an associated save or transaction request.
     *
     * @param tempDir
     *            File
     * @param tempDirPrefix
     *            String
     * @param tempFilePrefix
     *            String
     * @param jarFilePrefix
     *            String
     * @return FileRequestQueue
     * @throws DBException
     */
    @Override
    public FileRequestQueue performQueueCleanup(final File tempDir, final String tempDirPrefix, final String tempFilePrefix, final String jarFilePrefix)
            throws DBException {

        // Get a connection to the database

        Connection conn = null;
        PreparedStatement pstmt = null;
        Statement stmt = null;

        final FileRequestQueue reqQueue = new FileRequestQueue();

        try {

            // Get a connection to the database

            conn = getConnection(DBConnectionPool.PermanentLease);

            // Delete all load requests from the queue

            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + getQueueTableName() + " WHERE ReqType = " + FileRequest.LOAD + ";");

            while (rs.next()) {

                // Get the path to the cache file

                final String tempPath = rs.getString("TempFile");

                // Check if the cache file exists, the file load may have been in progress

                final File tempFile = new File(tempPath);
                if (tempFile.exists()) {

                    // Delete the cache file for the load request

                    tempFile.delete();

                    // DEBUG

                    if (hasDebug()) {
                        LOGGER.info("[mySQL] Deleted load request file " + tempPath);
                    }
                }
            }

            // Check if the lock file exists, if so then the server did not shutdown cleanly

            final File lockFile = new File(tempDir, LockFileName);
            setLockFile(lockFile.getAbsolutePath());

            final boolean cleanShutdown = lockFile.exists() == false;

            // Create a crash recovery folder if the server did not shutdown clean

            File crashFolder = null;

            if (cleanShutdown == false && hasCrashRecovery()) {

                // Create a unique crash recovery sub-folder in the temp area

                final SimpleDateFormat dateFmt = new SimpleDateFormat("yyyyMMMdd_HHmmss");
                crashFolder = new File(tempDir, "CrashRecovery_" + dateFmt.format(new Date(System.currentTimeMillis())));
                if (crashFolder.mkdir() == true) {
                    if (hasDebug()) {
                        LOGGER.debug("[mySQL] Created crash recovery folder - {}", crashFolder.getAbsolutePath());
                    }
                } else {
                    // Use the top level temp area for the crash recovery files
                    crashFolder = tempDir;
                    if (hasDebug()) {
                        LOGGER.debug("[mySQL] Failed to created crash recovery folder, using folder - " + crashFolder.getAbsolutePath());
                    }
                }
            }

            // Delete the file load request records

            stmt.execute("DELETE FROM " + getQueueTableName() + " WHERE ReqType = " + FileRequest.LOAD + ";");

            // Create a statement to check if a temporary file is part of a save request

            pstmt = conn.prepareStatement("SELECT FileId,SeqNo FROM " + getQueueTableName() + " WHERE TempFile = ?;");

            // Scan all files/sub-directories within the temporary area looking for files that have
            // been saved but not
            // deleted due to a server shutdown or crash.

            final File[] tempFiles = tempDir.listFiles();

            if (tempFiles != null && tempFiles.length > 0) {

                // Scan the file loader sub-directories for temporary files

                for (final File curFile : tempFiles) {

                    // Get the current file/sub-directory

                    if (curFile.isDirectory() && curFile.getName().startsWith(tempDirPrefix)) {

                        // Check if the sub-directory has any loader temporary files

                        final File[] subFiles = curFile.listFiles();

                        if (subFiles != null && subFiles.length > 0) {

                            // Check each file to see if it has a pending save request in the file
                            // request database

                            for (final File ldrFile : subFiles) {

                                // Get the current file from the list

                                if (ldrFile.isFile() && ldrFile.getName().startsWith(tempFilePrefix)) {

                                    try {

                                        // Get the file details from the file system table

                                        pstmt.clearParameters();
                                        pstmt.setString(1, ldrFile.getAbsolutePath());

                                        rs = pstmt.executeQuery();

                                        if (rs.next()) {

                                            // File save request exists for temp file, nothing to do

                                        } else {

                                            // Check if the modified date indicates the file may
                                            // have been updated

                                            if (ldrFile.lastModified() != 0L) {

                                                // Get the file id from the cache file name

                                                final String fname = ldrFile.getName();
                                                final int dotPos = fname.indexOf('.');
                                                final String fidStr = fname.substring(tempFilePrefix.length(), dotPos);

                                                if (fidStr.indexOf('_') == -1) {

                                                    // Convert the file id

                                                    int fid = -1;

                                                    try {
                                                        fid = Integer.parseInt(fidStr);
                                                    } catch (final NumberFormatException ex) {
                                                    }

                                                    // Get the file details from the database

                                                    if (fid != -1) {

                                                        // Get the file details for the temp file
                                                        // using the file id

                                                        rs = stmt.executeQuery("SELECT * FROM " + getFileSysTableName() + " WHERE FileId = " + fid + ";");

                                                        // If the previous server shutdown was clean
                                                        // then we may be able to queue the file
                                                        // save

                                                        if (cleanShutdown == true) {

                                                            if (rs.next()) {

                                                                // Get the currently stored modifed
                                                                // date and file size for the
                                                                // associated file

                                                                final long dbModDate = rs.getLong("ModifyDate");
                                                                final long dbFileSize = rs.getLong("FileSize");

                                                                // Check if the temp file requires
                                                                // saving

                                                                if (ldrFile.length() != dbFileSize || ldrFile.lastModified() > dbModDate) {

                                                                    // Build the filesystem path to
                                                                    // the file

                                                                    final String filesysPath = buildPathForFileId(fid, stmt);

                                                                    if (filesysPath != null) {

                                                                        // Create a file state for
                                                                        // the file

                                                                        final FileState fstate = m_dbCtx.getStateCache().findFileState(filesysPath, true);

                                                                        FileSegmentInfo fileSegInfo = (FileSegmentInfo) fstate
                                                                                .findAttribute(ObjectIdFileLoader.DBFileSegmentInfo);
                                                                        FileSegment fileSeg = null;

                                                                        if (fileSegInfo == null) {

                                                                            // Create a new file
                                                                            // segment

                                                                            fileSegInfo = new FileSegmentInfo();
                                                                            fileSegInfo.setTemporaryFile(ldrFile.getAbsolutePath());

                                                                            fileSeg = new FileSegment(fileSegInfo, true);
                                                                            fileSeg.setStatus(FileSegmentInfo.SaveWait, true);

                                                                            // Add the segment to
                                                                            // the file state cache

                                                                            fstate.addAttribute(ObjectIdFileLoader.DBFileSegmentInfo, fileSegInfo);

                                                                            // Add a file save
                                                                            // request for the temp
                                                                            // file to the recovery
                                                                            // queue

                                                                            reqQueue.addRequest(new SingleFileRequest(FileRequest.SAVE, fid, 0,
                                                                                    ldrFile.getAbsolutePath(), filesysPath, fstate));

                                                                            // Update the file size
                                                                            // and modified
                                                                            // date/time in the
                                                                            // filesystem database

                                                                            stmt.execute("UPDATE " + getFileSysTableName() + " SET FileSize = "
                                                                                    + ldrFile.length() + ", ModifyDate = " + ldrFile.lastModified()
                                                                                    + " WHERE FileId = " + fid + ";");

                                                                            // DEBUG

                                                                            if (hasDebug()) {
                                                                                LOGGER.info("[mySQL] Queued save request for " + ldrFile.getName() + ", path="
                                                                                        + filesysPath + ", fid=" + fid);
                                                                            }
                                                                        }
                                                                    } else {

                                                                        // Delete the temp file,
                                                                        // cannot resolve the path

                                                                        ldrFile.delete();

                                                                        // DEBUG

                                                                        if (hasDebug()) {
                                                                            LOGGER.info("[mySQL] Cannot resolve filesystem path for FID " + fid
                                                                                    + ", deleted file " + ldrFile.getName());
                                                                        }
                                                                    }
                                                                }
                                                            } else {

                                                                // Delete the temp file, file does
                                                                // not exist in the filesystem table

                                                                ldrFile.delete();

                                                                // DEBUG

                                                                if (hasDebug()) {
                                                                    LOGGER.info("[mySQL] No matching file record for FID " + fid + ", deleted file "
                                                                            + ldrFile.getName());
                                                                }
                                                            }
                                                        } else {

                                                            // File server did not shutdown cleanly
                                                            // so move any modified files to a
                                                            // holding area as they may be corrupt

                                                            if (rs.next() && hasCrashRecovery()) {

                                                                // Get the filesystem file name

                                                                final String extName = rs.getString("FileName");

                                                                // Generate a file name to rename
                                                                // the cache file into a crash
                                                                // recovery folder

                                                                final File crashFile = new File(crashFolder, "" + fid + "_" + extName);

                                                                // Rename the cache file into the
                                                                // crash recovery folder

                                                                if (ldrFile.renameTo(crashFile)) {

                                                                    // DEBUG

                                                                    if (LOGGER.isDebugEnabled() && hasDebug()) {
                                                                        LOGGER.debug("[mySQL] Crash recovery file - " + crashFile.getAbsolutePath());
                                                                    }
                                                                }
                                                            } else {

                                                                // DEBUG

                                                                if (LOGGER.isDebugEnabled() && hasDebug()) {
                                                                    LOGGER.debug("[mySQL] Deleted incomplete cache file - " + ldrFile.getAbsolutePath());
                                                                }

                                                                // Delete the incomplete cache file

                                                                ldrFile.delete();
                                                            }
                                                        }
                                                    } else {

                                                        // Invalid file id format, delete the temp
                                                        // file

                                                        ldrFile.delete();

                                                        // DEBUG

                                                        if (hasDebug()) {
                                                            LOGGER.info("[mySQL] Bad file id format, deleted file, " + ldrFile.getName());
                                                        }
                                                    }
                                                } else {

                                                    // Delete the temp file as it is for an NTFS
                                                    // stream

                                                    ldrFile.delete();

                                                    // DEBUG

                                                    if (hasDebug()) {
                                                        LOGGER.info("[mySQL] Deleted NTFS stream temp file, " + ldrFile.getName());
                                                    }
                                                }
                                            } else {

                                                // Delete the temp file as it has not been modified
                                                // since it was loaded

                                                ldrFile.delete();

                                                // DEBUG

                                                if (hasDebug()) {
                                                    LOGGER.info("[mySQL] Deleted unmodified temp file, " + ldrFile.getName());
                                                }
                                            }
                                        }
                                    } catch (final SQLException ex) {
                                        LOGGER.error(ex.getMessage(), ex);
                                    }
                                } else {

                                    // DEBUG

                                    if (hasDebug()) {
                                        LOGGER.info("[mySQL] Deleted temporary file " + ldrFile.getName());
                                    }

                                    // Delete the temporary file

                                    ldrFile.delete();
                                }
                            }
                        }
                    }
                }
            }

            // Create the lock file, delete any existing lock file

            if (lockFile.exists()) {
                lockFile.delete();
            }

            try {
                lockFile.createNewFile();
            } catch (final IOException ex) {

                // DEBUG

                if (LOGGER.isDebugEnabled() && hasDebug()) {
                    LOGGER.debug("[mySQL] Failed to create lock file - " + lockFile.getAbsolutePath());
                }
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }
        } finally {

            // Close the load request statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Close the prepared statement

            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // DEBUG

        if (hasDebug()) {
            LOGGER.info("[mySQL] Cleanup recovered " + reqQueue.numberOfRequests() + " file saves from previous run");
        }

        // Return the recovery file request queue

        return reqQueue;
    }

    /**
     * Check if the specified temporary file has a queued request.
     *
     * @param tempFile
     *            String
     * @param lastFile
     *            boolean
     * @return boolean
     * @exception DBException
     */
    public boolean hasQueuedRequest(final String tempFile, final boolean lastFile) throws DBException {

        Connection conn = null;
        Statement stmt = null;

        boolean queued = false;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            String sql = "SELECT FileId FROM " + getQueueTableName() + " WHERE TempFile = '" + tempFile + "';";

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Has queued req SQL: {}", sql);
            }

            // Check if there is a queued request using the temporary file

            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                queued = true;
            } else {

                // Check if there is a transaction using the temporary file

                sql = "SELECT FileId FROM " + getTransactionTableName() + " WHERE TempFile = '" + tempFile + "';";

                // DEBUG

                if (hasSQLDebug()) {
                    LOGGER.info("[mySQL] Has queued req SQL: " + sql);
                }

                // Check the transaction table

                rs = stmt.executeQuery(sql);
                if (rs.next()) {
                    queued = true;
                }
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }

            // Rethrow the exception

            throw new DBException(ex.getMessage());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final SQLException ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the queued status

        return queued;
    }

    /**
     * Delete a file request from the pending queue.
     *
     * @param fileReq
     *            FileRequest
     * @exception DBException
     */
    @Override
    public void deleteFileRequest(final FileRequest fileReq) throws DBException {

        Connection conn = null;
        Statement stmt = null;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            // Delete the file request queue entry from the request table or multiple records from
            // the
            // transaction table

            if (fileReq instanceof SingleFileRequest) {

                // Get the single file request details

                final SingleFileRequest singleReq = (SingleFileRequest) fileReq;

                // Delete the request record

                stmt.executeUpdate("DELETE FROM " + getQueueTableName() + " WHERE SeqNo = " + singleReq.getSequenceNumber());
            } else {

                // Delete the transaction records

                stmt.executeUpdate("DELETE FROM " + getTransactionTableName() + " WHERE TranId = " + fileReq.getTransactionId());
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }

            // Rethrow the exception

            throw new DBException(ex.getMessage());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final SQLException ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }
    }

    /**
     * Load a block of file request from the database into the specified queue.
     *
     * @param fromSeqNo
     *            int
     * @param reqType
     *            int
     * @param reqQueue
     *            FileRequestQueue
     * @param recLimit
     *            int
     * @return int
     * @exception DBException
     */
    @Override
    public int loadFileRequests(final int fromSeqNo, final int reqType, final FileRequestQueue reqQueue, final int recLimit) throws DBException {

        // Load a block of file requests from the loader queue

        Connection conn = null;
        Statement stmt = null;

        int recCnt = 0;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            // Build the SQL to load the queue records

            final String sql = "SELECT * FROM " + getQueueTableName() + " WHERE SeqNo > " + fromSeqNo + " AND ReqType = " + reqType + " ORDER BY SeqNo LIMIT "
                    + recLimit + ";";

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Load file requests - " + sql);
            }

            // Get a block of file request records

            final ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {

                // Get the file request details

                final int fid = rs.getInt("FileId");
                final int stid = rs.getInt("StreamId");
                final int reqTyp = rs.getInt("ReqType");
                final int seqNo = rs.getInt("SeqNo");
                final String tempPath = rs.getString("TempFile");
                final String virtPath = rs.getString("VirtualPath");
                final String attribs = rs.getString("Attribs");

                // Recreate the file request for the in-memory queue

                final SingleFileRequest fileReq = new SingleFileRequest(reqTyp, fid, stid, tempPath, virtPath, seqNo, null);
                fileReq.setAttributes(attribs);

                // Add the request to the callers queue

                reqQueue.addRequest(fileReq);

                // Update the count of loaded requests

                recCnt++;
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }

            // Rethrow the exception

            throw new DBException(ex.getMessage());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final SQLException ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the count of file requests loaded

        return recCnt;
    }

    /**
     * Load a transaction request from the queue.
     *
     * @param tranReq
     *            MultiplFileRequest
     * @return MultipleFileRequest
     * @exception DBException
     */
    @Override
    public MultipleFileRequest loadTransactionRequest(final MultipleFileRequest tranReq) throws DBException {

        // Load a transaction request from the transaction loader queue

        Connection conn = null;
        Statement stmt = null;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            final String sql = "SELECT * FROM " + getTransactionTableName() + " WHERE TranId = " + tranReq.getTransactionId() + ";";

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Load trans request - {}", sql);
            }

            // Get the block of file request records for the current transaction

            final ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {

                // Get the file request details

                final int fid = rs.getInt("FileId");
                final int stid = rs.getInt("StreamId");
                final String tempPath = rs.getString("TempFile");
                final String virtPath = rs.getString("VirtualPath");

                // Create the cached file information and add to the request

                final CachedFileInfo finfo = new CachedFileInfo(fid, stid, tempPath, virtPath);
                tranReq.addFileInfo(finfo);
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }

            // Rethrow the exception

            throw new DBException(ex.getMessage());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final SQLException ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the updated file request

        return tranReq;
    }

    /**
     * Shutdown the database interface, release resources.
     *
     * @param context
     *            DBDeviceContext
     */
    @Override
    public void shutdownDatabase(final DBDeviceContext context) {

        // Call the base class

        super.shutdownDatabase(context);
    }

    /**
     * Get the retention expiry date/time for a file/folder
     *
     * @param conn
     *            Connection
     * @param stmt
     *            Statement
     * @param fid
     *            int
     * @return RetentionDetails
     * @exception SQLException
     */
    private final RetentionDetails getRetentionExpiryDateTime(final Connection conn, final Statement stmt, final int fid) throws SQLException {

        // Get the retention expiry date/time for the specified file/folder

        RetentionDetails retDetails = null;
        final String sql = "SELECT StartDate,EndDate FROM " + getRetentionTableName() + " WHERE FileId = " + fid + ";";

        // DEBUG

        if (hasSQLDebug()) {
            LOGGER.debug("[mySQL] Get retention expiry SQL: {}", sql);
        }

        // Get the retention record, if any

        final ResultSet rs = stmt.executeQuery(sql);

        if (rs.next()) {

            // Get the retention expiry date

            final Timestamp startDate = rs.getTimestamp("StartDate");
            final Timestamp endDate = rs.getTimestamp("EndDate");

            retDetails = new RetentionDetails(fid, startDate != null ? startDate.getTime() : -1L, endDate.getTime());
        }

        // Return the retention expiry date/time

        return retDetails;
    }

    /**
     * Create the prepared statements used by the file request queueing database
     *
     * @exception SQLException
     */
    protected final void createQueueStatements() throws SQLException {

        // Check if the database connection is valid

        if (m_dbConn != null) {

            // Close the existing statements

            if (m_reqStmt != null) {
                m_reqStmt.close();
            }

            if (m_tranStmt != null) {
                m_tranStmt.close();
            }

            // Release the current database connection

            releaseConnection(m_dbConn);
            m_dbConn = null;

        }

        if (m_dbConn == null) {
            m_dbConn = getConnection(DBConnectionPool.PermanentLease);
        }

        // Create the prepared statements for accessing the file request queue database

        m_reqStmt = m_dbConn
                .prepareStatement("INSERT INTO " + getQueueTableName() + "(FileId,StreamId,ReqType,TempFile,VirtualPath,Attribs) VALUES (?,?,?,?,?,?);");

        // Create the prepared statements for accessing the transaction request queue database

        m_tranStmt = m_dbConn.prepareStatement(
                "INSERT INTO " + getTransactionTableName() + "(FileId,StreamId,ReqType,TranId,TempFile,VirtualPath,Attribs) VALUES (?,?,?,?,?,?,?);");
    }

    /**
     * Return the file data details for the specified file or stream.
     *
     * @param fileId
     *            int
     * @param streamId
     *            int
     * @return DBDataDetails
     * @throws DBException
     */
    @Override
    public DBDataDetails getFileDataDetails(final int fileId, final int streamId) throws DBException {

        // Load the file details from the data table

        Connection conn = null;
        Statement stmt = null;

        DBDataDetails dbDetails = null;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            final String sql = "SELECT * FROM " + getDataTableName() + " WHERE FileId = " + fileId + " AND StreamId = " + streamId + " AND FragNo = 1;";

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Get file data details SQL: {}", sql);
            }

            // Load the file details

            final ResultSet rs = stmt.executeQuery(sql);

            if (rs.next()) {

                // Create the file details

                dbDetails = new DBDataDetails(fileId, streamId);

                if (rs.getBoolean("JarFile") == true) {
                    dbDetails.setJarId(rs.getInt("JarId"));
                }
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }

            // Rethrow the exception

            throw new DBException(ex.getMessage());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final SQLException ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // If the file details are not valid throw an exception

        if (dbDetails == null) {
            throw new DBException("Failed to load file details for " + fileId + ":" + streamId);
        }

        // Return the file data details

        return dbDetails;
    }

    /**
     * Return the maximum data fragment size supported
     *
     * @return long
     */
    @Override
    public long getMaximumFragmentSize() {
        return 20 * MemorySize.MEGABYTE;
    }

    /**
     * Load file data from the database into a temporary/local file
     *
     * @param fileId
     *            int
     * @param streamId
     *            int
     * @param fileSeg
     *            FileSegment
     * @throws DBException
     * @throws IOException
     */
    @Override
    public void loadFileData(final int fileId, final int streamId, final FileSegment fileSeg) throws DBException, IOException {

        // Open the temporary file

        final FileOutputStream fileOut = new FileOutputStream(fileSeg.getTemporaryFile());

        // Update the segment status

        fileSeg.setStatus(FileSegmentInfo.Loading);

        // DEBUG

        long startTime = 0L;

        if (LOGGER.isInfoEnabled() && hasDebug()) {
            startTime = System.currentTimeMillis();
        }

        // Load the file data fragments

        Connection conn = null;
        Statement stmt = null;

        try {

            // Get a connection to the database, create a statement

            conn = getConnection();
            stmt = conn.createStatement();

            final String sql = "SELECT * FROM " + getDataTableName() + " WHERE FileId = " + fileId + " AND StreamId = " + streamId + " ORDER BY FragNo";

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Load file data SQL: {}", sql);
            }

            // Find the data fragments for the file, check if the file is stored in a Jar

            final ResultSet rs = stmt.executeQuery(sql);

            // Load the file data from the main file record(s)

            byte[] inbuf = null;
            int buflen = 0;
            int fragNo = -1;
            long totLen = 0L;

            while (rs.next()) {

                // Access the file data

                final Blob dataBlob = rs.getBlob("Data");
                fragNo = rs.getInt("FragNo");
                rs.getInt("FragLen");

                final InputStream dataFrag = dataBlob.getBinaryStream();

                // Allocate the read buffer, if not already allocated

                if (inbuf == null) {
                    buflen = (int) Math.min(dataBlob.length(), MaxMemoryBuffer);
                    inbuf = new byte[buflen];
                }

                // Read the data from the database record and write to the output file

                int rdLen = dataFrag.read(inbuf, 0, inbuf.length);

                while (rdLen > 0) {

                    // Write a block of data to the temporary file segment

                    fileOut.write(inbuf, 0, rdLen);
                    totLen += rdLen;

                    // Read another block of data

                    rdLen = dataFrag.read(inbuf, 0, inbuf.length);
                }

                // Signal to waiting threads that data is available

                fileSeg.setReadableLength(totLen);
                fileSeg.signalDataAvailable();

                // Renew the lease on the database connection

                getConnectionPool().renewLease(conn);
            }

            // Close the resultset

            rs.close();

            // DEBUG

            if (LOGGER.isInfoEnabled() && hasDebug()) {
                final long endTime = System.currentTimeMillis();
                LOGGER.info("[mySQL] Loaded fid=" + fileId + ", stream=" + streamId + ", frags=" + fragNo + ", time=" + (endTime - startTime) + "ms");
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }

            // Rethrow the exception

            throw new DBException(ex.getMessage());
        } finally {

            // Check if a statement was allocated

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }

            // Close the output file

            if (fileOut != null) {
                try {
                    fileOut.close();
                } catch (final Exception ex) {
                    LOGGER.error(ex.getMessage(), ex);
                }
            }
        }

        // Signal that the file data is available

        fileSeg.signalDataAvailable();
    }

    /**
     * Load Jar file data from the database into a temporary file
     *
     * @param jarId
     *            int
     * @param jarSeg
     *            FileSegment
     * @throws DBException
     * @throws IOException
     */
    @Override
    public void loadJarData(final int jarId, final FileSegment jarSeg) throws DBException, IOException {

        // Load the Jar file data

        Connection conn = null;
        Statement stmt = null;

        FileOutputStream outJar = null;

        try {

            // Get a connection to the database, create a statement

            conn = getConnection();
            stmt = conn.createStatement();

            final String sql = "SELECT * FROM " + getJarDataTableName() + " WHERE JarId = " + jarId;

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Load Jar data SQL: " + sql);
            }

            // Create the temporary Jar file

            outJar = new FileOutputStream(jarSeg.getTemporaryFile());

            // Get the Jar data record

            final ResultSet rs = stmt.executeQuery(sql);

            if (rs.next()) {

                // Access the Jar file data

                final Blob dataBlob = rs.getBlob("Data");
                final InputStream dataFrag = dataBlob.getBinaryStream();

                // Allocate the read buffer

                final byte[] inbuf = new byte[(int) Math.min(dataBlob.length(), MaxMemoryBuffer)];

                // Read the Jar data from the database record and write to the output file

                int rdLen = dataFrag.read(inbuf, 0, inbuf.length);
                while (rdLen > 0) {

                    // Write a block of data to the temporary file segment

                    outJar.write(inbuf, 0, rdLen);

                    // Read another block of data

                    rdLen = dataFrag.read(inbuf, 0, inbuf.length);
                }
            }

            // Close the output Jar file

            outJar.close();

            // Set the Jar file segment status to indicate that the data has been loaded

            jarSeg.setStatus(FileSegmentInfo.Available, false);
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }

            // Rethrow the exception

            throw new DBException(ex.getMessage());
        } finally {

            // Check if a statement was allocated

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }

            // Close the output file

            if (outJar != null) {
                try {
                    outJar.close();
                } catch (final Exception ex) {
                    LOGGER.error(ex.getMessage(), ex);
                }
            }
        }
    }

    /**
     * Save the file data from the temporary/local file to the database
     *
     * @param fileId
     *            int
     * @param streamId
     *            int
     * @param fileSeg
     *            FileSegment
     * @return int
     * @throws DBException
     * @throws IOException
     */
    @Override
    public int saveFileData(final int fileId, final int streamId, final FileSegment fileSeg) throws DBException, IOException {

        // Determine if we can use an in memory buffer to copy the file fragments

        boolean useMem = false;
        byte[] memBuf = null;

        if (getDataFragmentSize() <= MaxMemoryBuffer) {

            // Use a memory buffer to copy the file data fragments

            useMem = true;
            memBuf = new byte[(int) getDataFragmentSize()];
        }

        // Get the temporary file size

        final File tempFile = new File(fileSeg.getTemporaryFile());

        // Save the file data

        Connection conn = null;
        Statement delStmt = null;
        PreparedStatement stmt = null;
        int fragNo = 1;

        FileInputStream inFile = null;

        try {

            // Open the temporary file

            inFile = new FileInputStream(tempFile);

            // Get a connection to the database

            conn = getConnection();
            delStmt = conn.createStatement();

            final String sql = "DELETE FROM " + getDataTableName() + " WHERE FileId = " + fileId + " AND StreamId = " + streamId;

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Save file data SQL: " + sql);
            }

            // Delete any existing file data records for this file

            delStmt.executeUpdate(sql);
            delStmt.close();
            delStmt = null;

            // Add the file data to the database

            stmt = conn.prepareStatement("INSERT INTO " + getDataTableName() + " (FileId,StreamId,FragNo,FragLen,Data) VALUES (?,?,?,?,?)");

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Save file data SQL: " + stmt.toString());
            }

            long saveSize = tempFile.length();

            while (saveSize > 0) {

                // Determine the current fragment size to store

                long fragSize = Math.min(saveSize, getDataFragmentSize());

                // Determine if the data fragment should be copied to a memory buffer or a seperate
                // temporary file

                InputStream fragStream = null;

                if (saveSize == fragSize) {

                    // Just copy the data from the temporary file, only one fragment

                    fragStream = inFile;
                } else if (useMem == true) {

                    // Copy a block of data to the memory buffer

                    fragSize = inFile.read(memBuf);
                    fragStream = new ByteArrayInputStream(memBuf);
                } else {

                    // Need to create a temporary file and copy the fragment of data to it

                    throw new DBException("File data copy not implemented yet");
                }

                // Store the current fragment

                stmt.clearParameters();
                stmt.setInt(1, fileId);
                stmt.setInt(2, streamId);
                stmt.setInt(3, fragNo++);
                stmt.setInt(4, (int) fragSize);
                stmt.setBinaryStream(5, fragStream, (int) fragSize);

                if (stmt.executeUpdate() < 1 && hasDebug()) {
                    LOGGER.debug("## mySQL Failed to update file data, fid=" + fileId + ", stream=" + streamId + ", fragNo=" + (fragNo - 1));
                }

                // Update the remaining data size to be saved

                saveSize -= fragSize;

                // Renew the lease on the database connection so that it does not expire

                getConnectionPool().renewLease(conn);
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }

            // Rethrow the exception

            throw new DBException(ex.getMessage());
        } finally {

            // Close the delete statement

            if (delStmt != null) {
                try {
                    delStmt.close();
                } catch (final Exception ex) {
                }
            }

            // Close the insert statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }

            // Close the input file

            if (inFile != null) {
                try {
                    inFile.close();
                } catch (final Exception ex) {
                }
            }
        }

        // Return the number of data fragments used to save the file data

        return fragNo;
    }

    /**
     * Save the file data from a Jar file to the database
     *
     * @param jarPath
     *            String
     * @param fileList
     *            DBDataDetailsList
     * @return int
     * @throws DBException
     * @throws IOException
     */
    @Override
    public int saveJarData(final String jarPath, final DBDataDetailsList fileList) throws DBException, IOException {

        // Write the Jar file to the blob field in the Jar data table

        Connection conn = null;
        PreparedStatement istmt = null;
        Statement stmt = null;

        int jarId = -1;

        try {

            // Get a connection to the database

            conn = getConnection();

            // Open the Jar file

            final File jarFile = new File(jarPath);
            final FileInputStream inJar = new FileInputStream(jarFile);

            // Add the Jar file data to the database

            istmt = conn.prepareStatement("INSERT INTO " + getJarDataTableName() + " (Data) VALUES (?)");

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Save Jar data SQL: " + istmt.toString());
            }

            // Set the Jar data field

            istmt.setBinaryStream(1, inJar, (int) jarFile.length());

            if (istmt.executeUpdate() < 1 && hasDebug()) {
                LOGGER.debug("## mySQL Failed to store Jar data");
            }

            // Get the unique jar id allocated to the new Jar record

            stmt = conn.createStatement();
            final ResultSet rs = stmt.executeQuery("SELECT LAST_INSERT_ID();");

            if (rs.next()) {
                jarId = rs.getInt(1);
            }

            // Update the jar id record for each file in the Jar

            for (int i = 0; i < fileList.numberOfFiles(); i++) {

                // Get the current file details

                final DBDataDetails dbDetails = fileList.getFileAt(i);

                // Add the file data record(s) to the database

                stmt.executeUpdate(
                        "DELETE FROM " + getDataTableName() + " WHERE FileId = " + dbDetails.getFileId() + " AND StreamId = " + dbDetails.getStreamId());
                stmt.executeUpdate("INSERT INTO " + getDataTableName() + " (FileId,StreamId,FragNo,JarId,JarFile) VALUES (" + dbDetails.getFileId() + ","
                        + dbDetails.getStreamId() + ", 1," + jarId + ",1);");
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }

            // Rethrow the exception

            throw new DBException(ex.getMessage());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final SQLException ex) {
                }
            }

            // Close the insert statement

            if (istmt != null) {
                try {
                    istmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the allocated Jar id

        return jarId;
    }

    /**
     * Delete the file data for the specified file/stream
     *
     * @param fileId
     *            int
     * @param streamId
     *            int
     * @throws DBException
     * @throws IOException
     */
    @Override
    public void deleteFileData(final int fileId, final int streamId) throws DBException, IOException {

        // Delete the file data records for the file or stream

        Connection conn = null;
        Statement delStmt = null;

        try {

            // Get a connection to the database

            conn = getConnection();

            // Need to delete the existing data

            delStmt = conn.createStatement();
            String sql = null;

            // Check if the main file stream is being deleted, if so then delete all stream data too

            if (streamId == 0) {
                sql = "DELETE FROM " + getDataTableName() + " WHERE FileId = " + fileId;
            } else {
                sql = "DELETE FROM " + getDataTableName() + " WHERE FileId = " + fileId + " AND StreamId = " + streamId;
            }

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Delete file data SQL: " + sql);
            }

            // Delete the file data records

            final int recCnt = delStmt.executeUpdate(sql);

            // Debug

            if (hasDebug() && recCnt > 0) {
                LOGGER.info("[mySQL] Deleted file data fid=" + fileId + ", stream=" + streamId + ", records=" + recCnt);
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.info(ex.getMessage(), ex);
            }
        } finally {

            // Close the delete statement

            if (delStmt != null) {
                try {
                    delStmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }
    }

    /**
     * Delete the file data for the specified Jar file
     *
     * @param jarId
     *            int
     * @throws DBException
     * @throws IOException
     */
    @Override
    public void deleteJarData(final int jarId) throws DBException, IOException {
        // Delete the data records for the Jar file data
        Connection conn = null;
        Statement delStmt = null;
        try {
            // Get a connection to the database
            conn = getConnection();

            // Need to delete the existing data
            delStmt = conn.createStatement();
            final String sql = "DELETE FROM " + getJarDataTableName() + " WHERE JarId = " + jarId + ";";
            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Delete Jar data SQL: {}", sql);
            }

            // Delete the Jar data records
            final int recCnt = delStmt.executeUpdate(sql);
            if (hasDebug() && recCnt > 0) {
                LOGGER.info("[mySQL] Deleted Jar data jarId=" + jarId + ", records=" + recCnt);
            }
        } catch (final SQLException ex) {
            if (hasDebug()) {
                LOGGER.info(ex.getMessage(), ex);
            }
        } finally {
            // Close the delete statement
            if (delStmt != null) {
                try {
                    delStmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
            }
        }
    }

    // ***** DBObjectIdInterface Methods *****

    /**
     * Create a file id to object id mapping
     *
     * @param fileId
     *            int
     * @param streamId
     *            int
     * @param objectId
     *            String
     * @exception DBException
     */
    @Override
    public void saveObjectId(final int fileId, final int streamId, final String objectId) throws DBException {

        // Create a new file id/object id mapping record

        Connection conn = null;
        Statement stmt = null;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            // Delete any current mapping record for the object

            String sql = "DELETE FROM " + getObjectIdTableName() + " WHERE FileId = " + fileId + " AND StreamId = " + streamId;

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Save object id SQL: " + sql);
            }

            // Delete any current mapping record

            stmt.executeUpdate(sql);

            // Insert the new mapping record

            sql = "INSERT INTO " + getObjectIdTableName() + " (FileId,StreamId,ObjectID) VALUES(" + fileId + "," + streamId + ",'" + objectId + "')";

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Save object id SQL: " + sql);
            }

            // Create the mapping record

            if (stmt.executeUpdate(sql) == 0) {
                throw new DBException("Failed to add object id record, fid=" + fileId + ", objId=" + objectId);
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }

            // Rethrow the exception

            throw new DBException(ex.getMessage());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final SQLException ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }
    }

    /**
     * Load the object id for the specified file id
     *
     * @param fileId
     *            int
     * @param streamId
     *            int
     * @return String
     * @exception DBException
     */
    @Override
    public String loadObjectId(final int fileId, final int streamId) throws DBException {

        // Load the object id for the specified file id

        Connection conn = null;
        Statement stmt = null;

        String objectId = null;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            final String sql = "SELECT ObjectId FROM " + getObjectIdTableName() + " WHERE FileId = " + fileId + " AND StreamId = " + streamId;

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Load object id SQL: " + sql);
            }

            // Load the mapping record

            final ResultSet rs = stmt.executeQuery(sql);

            if (rs.next()) {
                objectId = rs.getString("ObjectId");
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }

            // Rethrow the exception

            throw new DBException(ex.getMessage());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final SQLException ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the object id

        return objectId;
    }

    /**
     * Delete a file id/object id mapping
     *
     * @param fileId
     *            int
     * @param streamId
     *            int
     * @param objectId
     *            String
     * @exception DBException
     */
    @Override
    public void deleteObjectId(final int fileId, final int streamId, final String objectId) throws DBException {

        // Delete a file id/object id mapping record

        Connection conn = null;
        Statement stmt = null;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            final String sql = "DELETE FROM " + getObjectIdTableName() + " WHERE FileId = " + fileId + " AND StreamId = " + streamId;

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Delete object id SQL: " + sql);
            }

            // Delete the mapping record

            stmt.executeUpdate(sql);
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }

            // Rethrow the exception

            throw new DBException(ex.getMessage());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final SQLException ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }
    }

    /**
     * Return the data for a symbolic link
     *
     * @param dirId
     *            int
     * @param fid
     *            int
     * @return String
     * @exception DBException
     */
    @Override
    public String readSymbolicLink(final int dirId, final int fid) throws DBException {

        // Delete a file id/object id mapping record

        Connection conn = null;
        Statement stmt = null;

        String symLink = null;

        try {

            // Get a connection to the database

            conn = getConnection();
            stmt = conn.createStatement();

            final String sql = "SELECT SymLink FROM " + getSymLinksTableName() + " WHERE FileId = " + fid;

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.debug("[mySQL] Read symbolic link: " + sql);
            }

            // Load the mapping record

            final ResultSet rs = stmt.executeQuery(sql);

            if (rs.next()) {
                symLink = rs.getString("SymLink");
            } else {
                throw new DBException("Failed to load symbolic link data for " + fid);
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }

            // Rethrow the exception

            throw new DBException(ex.getMessage());
        } finally {

            // Close the statement

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final SQLException ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }

        // Return the symbolic link data

        return symLink;
    }

    /**
     * Delete a symbolic link record
     *
     * @param dirId
     *            int
     * @param fid
     *            int
     * @exception DBException
     */
    @Override
    public void deleteSymbolicLinkRecord(final int dirId, final int fid) throws DBException {

        // Delete the symbolic link record for a file

        Connection conn = null;
        Statement delStmt = null;

        try {

            // Get a connection to the database

            conn = getConnection();
            delStmt = conn.createStatement();

            final String sql = "DELETE FROM " + getSymLinksTableName() + " WHERE FileId = " + fid;

            // DEBUG

            if (hasSQLDebug()) {
                LOGGER.info("[mySQL] Delete symbolic link SQL: " + sql);
            }

            // Delete the symbolic link record

            final int recCnt = delStmt.executeUpdate(sql);

            // Debug

            if (hasDebug() && recCnt > 0) {
                LOGGER.info("[mySQL] Deleted symbolic link fid=" + fid);
            }
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.info(ex.getMessage(), ex);
            }
        } finally {

            // Close the delete statement

            if (delStmt != null) {
                try {
                    delStmt.close();
                } catch (final Exception ex) {
                }
            }

            // Release the database connection

            if (conn != null) {
                releaseConnection(conn);
            }
        }
    }

    /**
     * Convert a file id to a share relative path
     *
     * @param fileid
     *            int
     * @param stmt
     *            Statement
     * @return String
     */
    private String buildPathForFileId(final int fileid, final Statement stmt) {

        // Build an array of folder names working back from the files id

        final StringList names = new StringList();

        try {

            // Loop, walking backwards up the tree until we hit root

            int curFid = fileid;

            do {

                // Search for the current file record in the database

                final ResultSet rs = stmt.executeQuery("SELECT DirId,FileName FROM " + getFileSysTableName() + " WHERE FileId = " + curFid + ";");

                if (rs.next()) {

                    // Get the filename

                    names.addString(rs.getString("FileName"));

                    // The directory id becomes the next file id to search for

                    curFid = rs.getInt("DirId");

                    // Close the resultset

                    rs.close();
                } else {
                    return null;
                }

            } while (curFid > 0);
        } catch (final SQLException ex) {

            // DEBUG

            if (hasDebug()) {
                LOGGER.error(ex.getMessage(), ex);
            }
            return null;
        }

        // Build the path string

        final StringBuffer pathStr = new StringBuffer(256);
        pathStr.append(FileName.DOS_SEPERATOR_STR);

        for (int i = names.numberOfStrings() - 1; i >= 0; i--) {
            pathStr.append(names.getStringAt(i));
            pathStr.append(FileName.DOS_SEPERATOR_STR);
        }

        // Remove the trailing slash from the path

        if (pathStr.length() > 0) {
            pathStr.setLength(pathStr.length() - 1);
        }

        // Return the path string

        return pathStr.toString();
    }
}
