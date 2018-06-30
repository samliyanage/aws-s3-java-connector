package com.sblsoft.aws.s3.util;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class S3Util {
	public static AmazonS3 getS3Client(final String region, final String accessKey, final String secretKey)throws Exception {
    	
    	AWSCredentials  credentials= new BasicAWSCredentials(accessKey, secretKey);
    	return AmazonS3Client.builder().withRegion(S3Util.getRegion(region)).withCredentials(new AWSStaticCredentialsProvider(credentials)).withForceGlobalBucketAccessEnabled(true).build();
	}
	
	private static Regions getRegion(String region) {
		region=region.replace('-', '_').toUpperCase();
		return Regions.valueOf(region);
	}
}
