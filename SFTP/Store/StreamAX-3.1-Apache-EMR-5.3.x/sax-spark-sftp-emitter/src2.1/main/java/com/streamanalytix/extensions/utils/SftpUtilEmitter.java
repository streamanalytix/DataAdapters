/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.utils;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** The Class SftpUtilEmitter.
 *
 * @author sax This is Utility class for Reading/Writing to SFTP server */
public final class SftpUtilEmitter {

    /** The Constant COM_DATABRICKS_SPARK_AVRO. */
    private static final String COM_DATABRICKS_SPARK_AVRO = "com.databricks.spark.avro";

    /** The Constant COM_DATABRICKS_SPARK_CSV. */
    private static final String COM_DATABRICKS_SPARK_CSV = "com.databricks.spark.csv";
    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(SftpUtilEmitter.class);

    /** This method will write a File to SFTP from incoming dataFrame.
     * 
     * @param userName
     *            for SFTP connection
     * @param password
     *            for SFTP connection
     * @param sftpHost
     *            for SFTP connection
     * @param location
     *            for SFTP connection
     * @param fileType
     *            is type of file we are going handle
     * @param dataFrame
     *            incoming
     * @param delayBtwConnectionRetires
     *            is basically max time for making a connection to SFTP
     * @param connectionRetries
     *            is number of times we can re-try for connection in case of time-out or any other network latency error */
    public static void writeToSftp(String userName, String password, String sftpHost, String location, String fileType, Dataset<Row> dataFrame,
            String delayBtwConnectionRetires, String connectionRetries) {
        LOGGER.info("entering : SftpUtil's  : writeToSftp method");
        StandardFileSystemManager manager = null;
        FileObject remoteFile;
        try {
            manager = new StandardFileSystemManager();
            LOGGER.debug(" Initializes the file manager ");
            manager.init();
            LOGGER.info("Going to handle : different type of file format : Json, Csv, Parquet, Avro ");
            switch (fileType) {
            // 1. we are here using coalesce to avoid data to be partitioned across nodes of cluster , this operation is need of further execution
            // 2. We need to read file again with single schema , in copyMerge(HDFS operation) we were getting schema / header scrapped
                case SftpConstant.Values.CSV:
                    dataFrame.coalesce(1).write().option("header", "true").option("inferSchema", "true").format(COM_DATABRICKS_SPARK_CSV)
                            .save(SftpConstant.Values.HDFS_WRITE_DF_PATH);
                    break;
                case SftpConstant.Values.AVRO:
                    dataFrame.coalesce(1).write().format(COM_DATABRICKS_SPARK_AVRO).save(SftpConstant.Values.HDFS_WRITE_DF_PATH);
                    break;
                case SftpConstant.Values.PARQUET:
                    dataFrame.coalesce(1).write().parquet(SftpConstant.Values.HDFS_WRITE_DF_PATH);
                    break;
                case SftpConstant.Values.JSON:
                    dataFrame.coalesce(1).write().json(SftpConstant.Values.HDFS_WRITE_DF_PATH);
                    break;
                default:
                    throw new IllegalArgumentException("File type is not valid ");
            } // switch ends
            LOGGER.debug("switch blocks ends , Written data to HDFS location  at :" + SftpConstant.Values.HDFS_WRITE_DF_PATH
                    + " w.r.t to file format given  ");

            remoteFile = retrySftpConnection(userName, password, sftpHost, location, fileType, manager, delayBtwConnectionRetires, connectionRetries);

            // Push all local files content to SFTP
            copyFilesOnSftpFromHdfs(remoteFile, manager, fileType);
        } catch (Exception exception) {
            LOGGER.error("Error came while writing data to SFTP: ", exception);
            throw new RuntimeException("Error came while writing data to SFTP: ", exception);
        } finally {
            manager.close();
        }

        LOGGER.info("exit from  : SftpUtil's  : writeToSftp method");
    }// writeToSftp ends here

    /** Copy files on sftp from hdfs.
     *
     * @param remoteFile
     *            is sftp remote file object
     * @param manager
     *            is File Manager
     * @param fileType
     *            is type of file */
    public static void copyFilesOnSftpFromHdfs(FileObject remoteFile, StandardFileSystemManager manager, String fileType) {
        LOGGER.info("entering   : SftpUtil's  : copyFilesOnSftpFromHdfs method");
        try {

            File file = new File(SftpConstant.Values.LOCAL_PATH_TO_WRITE_RDD);
            if (file.exists()) {
                FileUtils.deleteDirectory(file);
            }
            file.mkdir();

            FileSystem hdfsFs = FileSystem.get(new Configuration());
            // reading from hdfs to local
            hdfsFs.moveToLocalFile(new Path(SftpConstant.Values.HDFS_WRITE_DF_PATH), new Path(file.getAbsolutePath()));

            // Create local file object
            String tempFileLocation = file.getAbsolutePath() + File.separator + SftpConstant.Values.SFTP_WRITER_LOCAL_LOCATION;
            FileObject localFile = null;
            localFile = getFileWithTypeFilter(tempFileLocation, manager, fileType);

            // Copy local file to sftp server
            if (localFile != null) {
                remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
                LOGGER.info("Successfully upload all files from HDFS: " + SftpConstant.Values.HDFS_WRITE_DF_PATH + " to SFTP location");
            } else {
                throw new RuntimeException("Unable to find any file/content");
            }
            LOGGER.info("exit from : SftpUtil's  : copyFilesOnSftpFromHdfs method");
        } catch (Exception ex) {
            LOGGER.error("Error came while writing data to SFTP: ", ex);
            throw new RuntimeException("Error came while writing data to SFTP: ", ex);
        }

    }

    /** This method to read only actual file (ie avro , parquet, json , csv format only ) excluding _SUCCESS , metedata files coming from hdfs location
     * to local temp location before writing to sftp directory , will return FileObject.
     *
     * @param tempFileLocation
     *            temporary local file path
     * @param manager
     *            FileSystem Manager
     * @param fileType
     *            type of file
     * @return File Object
     * @throws FileSystemException
     *             the file system exception */
    private static FileObject getFileWithTypeFilter(String tempFileLocation, StandardFileSystemManager manager, String fileType)
            throws FileSystemException {
        LOGGER.info("entering   : SftpUtil's  : getFileWithTypeFilter method");
        File[] baseTemp = new File(tempFileLocation).listFiles();
        FileObject localFile = null;
        LOGGER.debug("we are filtering only data files copied from HDFS , exluding irrelevent files like metedata, _success files ");
        for (File file : baseTemp) {
            if (!file.isDirectory() && !file.isHidden() && !file.getName().contains("SUCCESS") && !file.getName().contains("metadata")) {
                localFile = manager.resolveFile(file.getAbsolutePath());
            }
        }
        LOGGER.debug(" SftpUtil's  : getFileWithTypeFilter method : files filtered successfully ");
        LOGGER.info("exit from    : SftpUtil's  : getFileWithTypeFilter method");
        return localFile;
    }

    /** This method will return remote FileObject after successful connection with SFTP server.
     * 
     * @param userName
     *            for SFTP connection
     * @param password
     *            for SFTP connection
     * @param host
     *            for SFTP connection
     * @param location
     *            for SFTP connection
     * @param manager
     *            File Manager
     * @param delayBtwConnectionRetires
     *            is basically max time for making a connection to SFTP
     * @return FileObject */
    private static FileObject getRemoteFileForSFTP(String userName, String password, String host, String location, StandardFileSystemManager manager,
            String delayBtwConnectionRetires) {

        LOGGER.info("entering   : SftpUtil's  : getRemoteFileForSFTP method");
        FileObject remoteFile = null;
        try {

            // Setup our SFTP configuration
            FileSystemOptions opts = new FileSystemOptions();
            SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(opts, "no");
            SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, true);
            int delayTime = Integer.parseInt(delayBtwConnectionRetires);
            // we are setting timeout delayTime millisec which will be provided by user from UI , we can change it accordingly w.r.t to Network
            // latency
            SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, delayTime);

            LOGGER.debug("  Create the SFTP URI using the host name, userid, password, remote :  ");
            // path and file name
            String sftpUri = "sftp://" + userName + ":" + password + "@" + host + location;
            LOGGER.debug(" SFTP URI created is :  " + sftpUri);

            // Create remote file object
            remoteFile = manager.resolveFile(sftpUri, opts);
            LOGGER.info("exit from   : SftpUtil's  : getRemoteFileForSFTP method");
        } catch (FileSystemException exc) {
            LOGGER.error("Re-Trying for SFTP connection AGAIN !!!!", exc);
        }
        return remoteFile;
    }

    /** This method will retry to get remote FileObject of SFTP.
     *
     * @param userName
     *            for SFTP connection
     * @param password
     *            for SFTP connection
     * @param sftpHost
     *            for SFTP connection
     * @param location
     *            for SFTP connection
     * @param fileType
     *            is type of file we will handle
     * @param manager
     *            is FileSystem manager
     * @param delayBtwConnectionRetires
     *            is basically max time for making a connection to SFTP
     * @param connectionRetries
     *            is number of times we can re-try for connection in case of time-out or any other network latency error
     * @return the file object */
    private static FileObject retrySftpConnection(String userName, String password, String sftpHost, String location, String fileType,
            StandardFileSystemManager manager, String delayBtwConnectionRetires, String connectionRetries) {

        LOGGER.info("entering   : SftpUtil's  : retrySftpConnection method");
        FileObject remoteFile = null;
        if (userName == null || password == null || sftpHost == null || location == null || fileType == null) {
            throw new RuntimeException("fields missing from UI or fields are null while reading from sftp or dataFrame is empty/Null");
        }
        try {
            LOGGER.debug(" Setup our SFTP configuration ");
            int count = 0;
            int connRetries = Integer.parseInt(connectionRetries);

            while (count <= connRetries) {
                remoteFile = getRemoteFileForSFTP(userName, password, sftpHost, location, manager, delayBtwConnectionRetires);
                if (remoteFile != null) {
                    break;
                }
                count++;
            }

            if (remoteFile == null) {
                throw new FileSystemException("Could not able to setup connection with SFTP server");
            }
            LOGGER.info("exit from   : SftpUtil's  : retrySftpConnection method");
        } catch (Exception ex) {
            LOGGER.error("Error came during Sftp connection ", ex);
            throw new RuntimeException("Error came during setup of Sftp connection ", ex);
        }
        return remoteFile;

    }

}
