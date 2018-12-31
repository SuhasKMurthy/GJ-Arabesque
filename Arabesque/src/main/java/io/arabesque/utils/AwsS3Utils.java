package io.arabesque.utils;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.File;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class AwsS3Utils implements Serializable {
    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withCredentials(new ProfileCredentialsProvider()).withForceGlobalBucketAccessEnabled(true)
            .build();

    public AwsS3Utils() {
        /*
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .build();
        */
    }

    private static String[] getPathInfo(String path) {
        path = path.replace("s3://","");
        path = path.replaceAll("s3.://","");
        String[] pathInfo = path.split("/");
        String bucketName = pathInfo[0];
        String key = String.join("/", Arrays.copyOfRange(pathInfo,1,pathInfo.length));
        String[] result = new String[2];
        result[0] = bucketName;
        result[1] = key;
        return result;
    }

    public InputStream readFromPath(String s3Path) {
        String[] pathInfo = getPathInfo(s3Path);
        String bucketName = pathInfo[0];
        String key = pathInfo[1];
        try {
            S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
            return fullObject.getObjectContent();
        }catch (Exception e) {
            System.out.println("Error reading path: " + s3Path);
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void uploadFile(String path, String s3Path) {
        String[] pathInfo = getPathInfo(s3Path);
        String bucketName = pathInfo[0];
        String key = pathInfo[1];
        PutObjectRequest request = new PutObjectRequest(bucketName, key, new File(path));
        s3Client.putObject(request);
    }

    private String buildPath(String bucketName, String key) {
        return "s3://" + bucketName + "/" + key;
    }

    public List<String> listFiles(String path) {
        String[] pathInfo = getPathInfo(path);
        String bucketName = pathInfo[0];
        String key = pathInfo[1];
        if(path.substring(path.length()-1).equals("/")) { key = key + "/"; }
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(key).withDelimiter("/");
        List<String> result = new LinkedList<>();
        ListObjectsV2Result keys;
        do {
            keys = s3Client.listObjectsV2(req);

            for (S3ObjectSummary objectSummary : keys.getObjectSummaries()) {
                if(objectSummary.getKey().equals(key)) { continue; }
                result.add(buildPath(bucketName, objectSummary.getKey()));
            }
            String token = keys.getNextContinuationToken();
            req.setContinuationToken(token);
        } while (keys.isTruncated());
        return result;
    }

    public void deleteFile(String s3Path) {
        String[] pathInfo = getPathInfo(s3Path);
        String bucketName = pathInfo[0];
        String key = pathInfo[1];
        s3Client.deleteObject(new DeleteObjectRequest(bucketName, key));
    }

    public static void main(String args[]) {
        String path = "s3://qfrag/graphs";
        path = path.replaceAll("s3.://","");
        System.out.println(path);
        //AwsS3Utils s3Object = new AwsS3Utils();
        //s3Object.listFiles("s3://qfrag/graphs/");
    }
}
