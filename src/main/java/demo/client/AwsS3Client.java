package demo.client;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

import com.amazonaws.xray.interceptors.TracingInterceptor;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

public class AwsS3Client {

	private S3Client client;
	
	public AwsS3Client(Region region) {
		
		Objects.requireNonNull(region);
		
		this.client = S3Client.builder()
				  .region(region)
				  .overrideConfiguration(builder -> builder
					.apiCallTimeout(Duration.ofMinutes(2))
					.addExecutionInterceptor(new TracingInterceptor())
					//.apiCallAttemptTimeout(/* 15 min */)
					//.retryPolicy(RetryMode.STANDARD)
					.build())
				  .build();
	}
	
	public AwsS3Client(AwsCredentialsProvider credentialsProvider, Region region) {
		
		this.client = S3Client.builder()
				  .credentialsProvider(credentialsProvider)
				  .region(region)
				  .overrideConfiguration(builder -> builder
					.apiCallTimeout(Duration.ofMinutes(2))
					//.apiCallAttemptTimeout(/* 15 min */)
					//.retryPolicy(RetryMode.STANDARD)
					.build())
				  .build();
	}
	
	public void listBucket() {
		ListBucketsRequest request = ListBucketsRequest.builder().build();
		ListBucketsResponse response = this.client.listBuckets(request);
		for (Bucket bucket : response.buckets()) {
			System.out.println(bucket.name());
		}
	}
	
	public void listObject(String bucket) {
		ListObjectsRequest request = ListObjectsRequest.builder().bucket(bucket).build();
		ListObjectsResponse response = this.client.listObjects(request);
		for (S3Object s3object : response.contents()) {
			System.out.println(s3object.key());
		}
	}
	
	public void getObject(String bucket, String key, Path path) {
		GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
		GetObjectResponse response = this.client.getObject(request, path);
		System.out.println(response);
	
	}
	
	public void putObject(String bucket, String key, Path path) {
		PutObjectRequest request = PutObjectRequest.builder().bucket(bucket).key(key).build();
		PutObjectResponse response = this.client.putObject(request, RequestBody.fromFile(path));
		System.out.println(response);
	}

}
