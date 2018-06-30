package com.sblsoft.aws.s3;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.vertrax.aws.s3.util.S3Util;

public class ObjectOperation {

	/**
	 * @param region
	 * @param accessKey
	 * @param secretKey
	 * @param bucketName
	 * @param filePath
	 * @param destinationDirectory
	 * @return boolean
	 */
	public boolean uploadObject(final String region, final String accessKey, final String secretKey,
			final String bucketName, final String filePath, final String destinationDirectory) {
		
		try {
			AmazonS3 s3 = S3Util.getS3Client(region, accessKey, secretKey);
			
			String keyName;
			if(destinationDirectory!=null && !"".equals(destinationDirectory.trim())) {
				keyName = destinationDirectory+File.separator+Paths.get(filePath).getFileName().toString();
			}else{
				keyName = Paths.get(filePath).getFileName().toString();
			}

			System.out.format("Uploading %s to S3 bucket %s...\n", filePath, bucketName);

			s3.putObject(bucketName, keyName, new File(filePath));
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			return false;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		System.out.println("Upload Done!");
		return true;
	}
	
	/**
	 * @param region
	 * @param accessKey
	 * @param secretKey
	 * @param bucketName
	 * @param putObjectRequest
	 * @return boolean
	 */
	public boolean uploadObject(final String region, final String accessKey, final String secretKey,
			final String bucketName, PutObjectRequest putObjectRequest) {
		
		try {
			final AmazonS3 s3 = S3Util.getS3Client(region, accessKey, secretKey);

			System.out.format("Uploading %s to S3 bucket %s...\n", putObjectRequest.getKey(), bucketName);

			s3.putObject(putObjectRequest.withCannedAcl(CannedAccessControlList.PublicRead));
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			return false;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		System.out.println("Upload Done!");
		return true;
	}
	
	/**
	 * @param region
	 * @param accessKey
	 * @param secretKey
	 * @param bucketName
	 * @param key
	 * @param input
	 * @param metadata
	 * @return boolean
	 */
	public boolean uploadObject(final String region, final String accessKey, final String secretKey,
			final String bucketName, final String key, final InputStream input, final ObjectMetadata metadata) {
		
		try {
			final AmazonS3 s3 = S3Util.getS3Client(region, accessKey, secretKey);

			System.out.format("Uploading %s to S3 bucket %s...\n", key, bucketName);

			s3.putObject(bucketName, key, input, metadata);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			return false;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		System.out.println("Upload Done!");
		return true;
	}
	
	/**
	 * @param region
	 * @param accessKey
	 * @param secretKey
	 * @param bucketName
	 * @return ListObjectsV2Result
	 * @throws Exception
	 */
	public ListObjectsV2Result listObjects(final String region, final String accessKey, final String secretKey,
			final String bucketName) throws Exception {

		System.out.format("Objects in S3 bucket %s:\n", bucketName);
		final AmazonS3 s3 = S3Util.getS3Client(region, accessKey, secretKey);

		return s3.listObjectsV2(bucketName);
	}
	
	/**
	 * @param region
	 * @param accessKey
	 * @param secretKey
	 * @param bucketName
	 * @param key
	 * @return S3Object
	 */
	public S3Object getObject(final String region, final String accessKey, final String secretKey,
			final String bucketName, final String key){

		S3Object object=null;
        System.out.format("Downloading %s from S3 bucket %s...\n", key, bucketName);
        try {
        	final AmazonS3 s3 = S3Util.getS3Client(region, accessKey, secretKey);
            object = s3.getObject(bucketName, key);
            return object;
        } catch (AmazonServiceException e) {
        	e.printStackTrace();
        	return object;
        } catch (Exception e) {
        	e.printStackTrace();
        	return object;
		} 
    }
	
	/**
	 * @param region
	 * @param accessKey
	 * @param secretKey
	 * @param sourceBucket
	 * @param destinationBucket
	 * @param sourceKey
	 * @param destinationKey
	 * @return boolean
	 */
	public boolean copyObject(final String region, final String accessKey, final String secretKey, final String sourceBucket, final String destinationBucket, final String sourceKey, final String destinationKey) {
		System.out.format("Copying object %s",sourceKey);
        try {
        	final AmazonS3 s3 = S3Util.getS3Client(region, accessKey, secretKey);
            s3.copyObject(sourceBucket, sourceKey, destinationBucket, destinationKey);
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return false;
        } catch (Exception e) {
        	 e.printStackTrace();
             return false;
		}
        System.out.println("Done!");
        return true;
        
	}
	
	/**
	 * @param region
	 * @param accessKey
	 * @param secretKey
	 * @param bucketName
	 * @param objectKey
	 * @return boolean
	 */
	public boolean deleteObject(final String region, final String accessKey, final String secretKey, final String bucketName, final String objectKey) {
	        System.out.format("Deleting object %s from S3 bucket: %s\n", objectKey,
	                bucketName);
	        try {
	        	final AmazonS3 s3 = S3Util.getS3Client(region, accessKey, secretKey);
	            s3.deleteObject(bucketName, objectKey);
	        } catch (AmazonServiceException e) {
	        	e.printStackTrace();
	            return false;
	        } catch (Exception e) {
	        	e.printStackTrace();
	            return false;
			}
	        System.out.println("Done!");
	        return true;
	}
	
	/**
	 * @param region
	 * @param accessKey
	 * @param secretKey
	 * @param bucketName
	 * @param directoryToClean
	 * @return boolean
	 */
	public boolean cleanupDirecoryContent(final String region, final String accessKey, final String secretKey, final String bucketName, final String directoryToClean) {
		try {
			final AmazonS3 s3 = S3Util.getS3Client(region, accessKey, secretKey);
			ObjectListing objectListing = s3.listObjects(bucketName);
			List<String> keysToDelete=new ArrayList<>();
			while (true) {
	            Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
	            while (objIter.hasNext()) {
	            	String key=objIter.next().getKey();
	            	if(key.startsWith(directoryToClean)) {
	            		keysToDelete.add(key);
	            	}
	            }

	            if (objectListing.isTruncated()) {
	                objectListing = s3.listNextBatchOfObjects(objectListing);
	            } else {
	                break;
	            }
	        }
			if(keysToDelete.size()>0) {
				String[] stockArr = new String[keysToDelete.size()];
	        	DeleteObjectsRequest dor = new DeleteObjectsRequest(bucketName).withKeys(keysToDelete.toArray(stockArr));
	        	s3.deleteObjects(dor);
	        }
		}catch(Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
		
	}
	
}
