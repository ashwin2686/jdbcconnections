/*
package com.pearson.daalt.qa.helpers;

//import com.amazonaws.athena.jdbc.shaded.com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
*/
/*

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * Created by vamarra on 6/5/15.
 *//*

public class AWSS3Helper {



    public String getObjectContents(String bucketName, String objectKey) {
        //AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        //S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, objectKey));
        //BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
        String line = null;
        String fileContents="";
        try {
            while ((line=reader.readLine()) != null) {
             fileContents+=line;
           }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return fileContents;
        }



}
*/
