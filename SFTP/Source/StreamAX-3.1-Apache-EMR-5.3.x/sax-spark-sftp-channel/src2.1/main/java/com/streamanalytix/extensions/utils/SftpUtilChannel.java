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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/** @author sax This is Utility class for Reading/Writing to SFTP server */
public final class SftpUtilChannel {
    /** The Constant serialVersionUID. */
    @SuppressWarnings("unused")
    private static final long serialVersionUID = -8657585005598090114L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(SftpUtilChannel.class);

    // private static SQLContext sqlContext;

    /** This method will read Files/File from SFTP server and return combined DataFrame.
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
     * @param context
     *            is the context of Spark
     * @param delayBtwConnectionRetires
     *            is basically max time for making a connection to SFTP
     * @param connectionRetries
     *            is number of times we can re-try for connection in case of time-out or any other network latency error
     * @return Dataframe we will return data frame after reading from SFTP
     * @throws FileSystemException */
    public static Dataset<Row> createDataFrameFromSftp(String userName, String password, String sftpHost, String location, String fileType,
            JavaSparkContext context, String delayBtwConnectionRetires, String connectionRetries) throws FileSystemException {

        LOGGER.info("entering : SftpUtil's  : readFromSftp");
        StandardFileSystemManager manager = null;
        Dataset dataFrame = null;
        FileObject remoteFile = null;
        try {

            SparkSession spark = SparkSession.builder().appName("SFTP_Channel").getOrCreate();

            // sqlContext = new SQLContext(context);
            manager = new StandardFileSystemManager();
            LOGGER.debug(" Initializes the file manager ");
            manager.init();
            remoteFile = retrySftpConnection(userName, password, sftpHost, location, fileType, manager, delayBtwConnectionRetires, connectionRetries);

            copyFilesOnHdfs(remoteFile, manager);

            // Below code is to handle different file format
            switch (fileType) {
                case SftpConstant.Values.CSV:
                    /*
                     * dataFrame = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true")
                     * .load(SftpConstant.Values.HDFS_CREATE_DF_PATH);
                     */
                    dataFrame = spark.read().format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true")
                            .load(SftpConstant.Values.HDFS_CREATE_DF_PATH);
                    break;
                case SftpConstant.Values.AVRO:
                    dataFrame = spark.read().format("com.databricks.spark.avro").load(SftpConstant.Values.HDFS_CREATE_DF_PATH);
                    break;
                case SftpConstant.Values.PARQUET:
                    dataFrame = spark.read().parquet(SftpConstant.Values.HDFS_CREATE_DF_PATH);
                    break;
                case SftpConstant.Values.JSON:
                    dataFrame = spark.read().json(SftpConstant.Values.HDFS_CREATE_DF_PATH);
                    break;
                default:
                    throw new IllegalArgumentException("File type is not valid ");
            }

        } catch (Exception exception) {
            LOGGER.error(" Error came while reading files from SFTP / moving to HDFS ", exception);
            throw new RuntimeException("Error came while reading files from SFTP / moving to HDFS", exception);
        } finally {
            manager.close();
        }
        return dataFrame;

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

    /** This method will copy files on HDFS.
     * 
     * @param remoteFile
     *            remote file object
     * @param manager
     *            File System Manager */
    private static void copyFilesOnHdfs(FileObject remoteFile, StandardFileSystemManager manager) {
        LOGGER.info("entering   : SftpUtil's  : copyFilesOnHdfs method");
        FileObject localFile = null;

        try {
            File file = new File(SftpConstant.Values.LOCAL_PATH);
            if (file.exists()) {
                FileUtils.deleteDirectory(file);
            }
            file.mkdir();
            FileSystem hdfsFs = FileSystem.get(new Configuration());
            // we need to handle Directory as well as Single file from SFTP while reading
            if (remoteFile.getType().name().equalsIgnoreCase("folder")) {

                // List all the files in that directory.Try to give the
                // directory path
                FileObject[] children = remoteFile.getChildren();
                int totalFiles = 0;
                for (FileObject fileObject : children) {
                    totalFiles++;
                    localFile = manager.resolveFile(file.getAbsolutePath() + File.separator + fileObject.getName().getBaseName());
                    localFile.copyFrom(fileObject, Selectors.SELECT_SELF);
                }
                if (totalFiles == 0) {
                    LOGGER.error(" SFTP directory is empty : No files present in SFTP location ");
                    throw new RuntimeException("SFTP directory is empty : No files present in SFTP location");
                }

            } else {
                localFile = manager.resolveFile(file.getAbsolutePath() + File.separator + remoteFile.getName().getBaseName());
                localFile.copyFrom(remoteFile, Selectors.SELECT_SELF);
            }
            LOGGER.info(" File download from SFTP to LOCAL successfully at location: " + file.getAbsolutePath());
            LOGGER.debug(" deleting already exists files if exist in HDFS ");

            if (hdfsFs.exists(new Path(SftpConstant.Values.HDFS_CREATE_DF_PATH))) {
                hdfsFs.delete(new Path(SftpConstant.Values.HDFS_CREATE_DF_PATH), true);
            }

            LOGGER.debug(" moving files from local to HDFS ");
            hdfsFs.moveFromLocalFile(new Path(file.getAbsolutePath()), new Path(SftpConstant.Values.HDFS_CREATE_DF_PATH));
            LOGGER.info("Successfully copied all files from SFTP to HDFS at location:" + SftpConstant.Values.HDFS_CREATE_DF_PATH);
            LOGGER.info("exit from   : SftpUtil's  : copyFilesOnHdfs method");
        } catch (Exception ex) {
            LOGGER.error("Error came while moving files on HDFS ", ex);
            throw new RuntimeException("Error came while moving files on HDFS ", ex);
        }
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
     * @return */
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
