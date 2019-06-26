package org.apache.hadoop.fs.s3a;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.List;

// Interface to declare all methods needed from S3AFileSystem here
public abstract class S3AFileSystemCommon extends FileSystem {

    public abstract ObjectMetadata getObjectMetadata(Path path) throws IOException;
    public abstract String getUsername();
    public abstract UploadInfo putObject(PutObjectRequest putObjectRequest);
    
    /**
     * PUT an object directly (i.e. not via the transfer manager).
     * Byte length is calculated from the file length, or, if there is no
     * file, from the content length of the header.
     * <i>Important: this call will close any input stream in the request.</i>
     * @param putObjectRequest the request
     * @return the upload initiated
     * @throws AmazonClientException on problems
     */
    abstract PutObjectResult putObjectDirect(PutObjectRequest putObjectRequest);

    public abstract UploadPartResult uploadPart(UploadPartRequest request);

    abstract void removeKeys(List<DeleteObjectsRequest.KeyVersion> keysToDelete,
                    boolean clearKeys, boolean deleteFakeDir)
                    throws MultiObjectDeleteException, AmazonClientException, InvalidRequestException;

    public abstract RemoteIterator<LocatedFileStatus> listFilesAndEmptyDirectories(Path f, boolean recursive) 
            throws IOException;
    
    public abstract void setWorkingDirectory(Path newDir);
    public abstract Path getWorkingDirectory();
    public abstract String getBucketLocation() throws IOException;
    public abstract String getBucketLocation(String bucketName) throws IOException;
    public abstract String getBucket();
    public abstract String pathToKey(Path path);
    public abstract Path qualify(Path path);

    public abstract ObjectMetadata newObjectMetadata();
    public abstract ObjectMetadata newObjectMetadata(long length);
    
    public abstract FSDataInputStream open(Path f, long file_length);

        /** NEW methods for Datanode S3 */
    
    // only used by testing
    @VisibleForTesting
    public abstract AmazonS3 getS3Client();
}
