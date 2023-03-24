package demo;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.ResourceBundle;

import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.AWSXRayRecorderBuilder;
import com.amazonaws.xray.entities.Subsegment;

import demo.client.AwsS3Client;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

public class BasicProcessor {

	public static void main(String[] args) {
		
		System.out.printf("Application Version: 1.8 漢字　%n");
		System.out.printf("Locale: %s　%n", Locale.getDefault());
		
		ResourceBundle resource = ResourceBundle.getBundle("application");
		//System.out.println(resource.getString("s3.bucket.name"));
		/*
		try {
			AWSXRayRecorderBuilder builder = AWSXRayRecorderBuilder
                    .standard();
			AWSXRay.setGlobalRecorder(builder.build());
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}*/
		
		System.out.printf("******************************************************** %n");
		
		AwsS3Client s3 = newS3ClientInstance();
		
		System.out.printf("******************************************************** %n");

		demoArguments(args);

		demoEnvironmentVariables();

		//demoSimulateWork();
		
		demoS3ListBucket(s3);
		
		demoS3ListObject(s3, resource);
		
		demoS3GetObject(s3, resource);
		
		demoS3PutObject(s3, resource);
		
	}
	
	private static void demoArguments(String[] args) {
		
		System.out.printf("Args.Length: %s %n", args.length);
		for (String arg : args) {
			System.out.printf("Arg[x]: %s %n", arg);
		}
		
		System.out.printf("******************************************************** %n");

	}
	
	private static void demoEnvironmentVariables() {
		
		System.out.printf("Env.APP_ENV: %s %n", System.getenv("APP_ENV"));
		System.out.printf("Env.APP_AWS_REGION: %s %n", System.getenv("APP_AWS_REGION"));
		System.out.printf("Env.APP_AWS_BUCKET_NAME: %s %n", System.getenv("APP_AWS_BUCKET_NAME"));
		System.out.printf("Env.APP_AWS_BUCKET_OBJECT_KEY: %s %n", System.getenv("APP_AWS_BUCKET_OBJECT_KEY"));
		System.out.printf("Env.APP_SERVICE_NAME: %s %n", System.getenv("APP_SERVICE_NAME"));
		System.out.printf("******************************************************** %n");

	}
	

	private static void demoS3ListBucket(AwsS3Client s3) {
		
		Subsegment subsegment = AWSXRay.beginSubsegment("Save Game");
		
		String awsRegion = System.getenv("APP_AWS_REGION");
		
		if (awsRegion == null || awsRegion.isBlank()) {
			System.out.printf("Skip Listing Buckets... %n");
			return;
		}

		try {
			
			System.out.printf("Listing All Buckets: Region=%s %n", awsRegion);

			s3.listBucket();
		
		} catch (Exception e) {
			subsegment.addException(e);
			//e.printStackTrace();
			//throw new RuntimeException(e);
			System.err.println("Failed to list all buckets. " + e.getMessage());
		} finally {
			AWSXRay.endSubsegment();
		}
		
		System.out.printf("******************************************************** %n");
	}
	
	private static void demoS3ListObject(AwsS3Client s3, ResourceBundle resource) {
		
		String awsRegion = System.getenv("APP_AWS_REGION");
		
		if (awsRegion == null || awsRegion.isBlank()) {
			System.out.printf("Skip Listing Objects... %n");
			return;
		}
		
		String bucket = resolveAwsBucketName(resource);

		try {

			System.out.printf("Listing Object: Region=%s Bucket=%s %n", awsRegion, bucket);
			
			s3.listObject(bucket);
		
		} catch (Exception e) {
			//e.printStackTrace();
			//throw new RuntimeException(e);
			System.err.println("Failed to list objects. " + e.getMessage());
		}
		
		System.out.printf("******************************************************** %n");
	}
	
	private static void demoS3GetObject(AwsS3Client s3, ResourceBundle resource) {
		
		String awsRegion = System.getenv("APP_AWS_REGION");
		
		if (awsRegion == null || awsRegion.isBlank()) {
			System.out.printf("Skip Get Object... %n");
			return;
		}
		
		String bucket = resolveAwsBucketName(resource);
		String key = resolveAwsBucketObjectKey(resource);
		
		try {
			
			System.out.printf("Get Object: Region=%s Bucket=%s Key=%s %n", awsRegion, bucket, key);
			
			Path path = Paths.get("data", key);
			
			Files.deleteIfExists(path);
			
			Path parent = path.getParent();
			System.out.printf("Get Object: Path=%s Parent=%s %n", path, parent);
			if (parent != null && !Files.exists(parent)) {
				Files.createDirectories(parent);
			}
			
			System.out.printf("Get Object: Listing data %n");
			Files.list(Paths.get("data")).forEach(System.out::println);
			
			s3.getObject(bucket, key, path);
			
			if (key.endsWith(".txt") || key.endsWith(".csv") || key.endsWith(".xml") || key.endsWith(".md")) {
				System.out.printf("--- Print File Content %s %n", path);
				try (BufferedReader reader = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {
					reader.lines().limit(5).forEach(line -> {
						System.out.println(line);
					});
				}
			}
		
		} catch (Exception e) {
			//e.printStackTrace();
			//throw new RuntimeException(e);
			System.err.println("Failed to get object. " + e.getMessage());
		}
		
		System.out.printf("******************************************************** %n");
	}

	private static void demoS3PutObject(AwsS3Client s3, ResourceBundle resource) {
		
		String awsRegion = System.getenv("APP_AWS_REGION");
		
		if (awsRegion == null || awsRegion.isBlank()) {
			System.out.printf("Skip Put Object... %n");
			return;
		}
		
		String bucket = resolveAwsOutBucketName(resource);
		
		if (bucket == null || bucket.isBlank()) {
			System.out.printf("Skip Put Object, Output Bucket Undefined... %n");
			return;
		}
		
		String key = String.format("outbound/batch-output-%s.txt", LocalDateTime.now());
		String serviceName = System.getenv("APP_SERVICE_NAME");
		if (serviceName == null || serviceName.isBlank()) {
			key = String.format("outbound/batch-output-%s.txt", LocalDateTime.now());
		} else {
			key = String.format("outbound/%s/batch-output-%s.txt", serviceName, LocalDateTime.now());
		}
		
		try {
			
			System.out.printf("Put Object: Region=%s Bucket=%s Key=%s %n", awsRegion, bucket, key);
			
			Path path = Paths.get("data", "input.txt");
			
			s3.putObject(bucket, key, path);
		
		} catch (Exception e) {
			//e.printStackTrace();
			//throw new RuntimeException(e);
			System.err.println("Failed to put object. " + e.getMessage());
		}
		
		System.out.printf("******************************************************** %n");
	}
	
	private static String resolveAwsBucketName(ResourceBundle resource) {
		
		String bucket = System.getenv("APP_AWS_BUCKET_NAME");
		if (bucket == null || bucket.isBlank()) {
			bucket = resource.getString("s3.bucket.name");
		}

		return bucket;

	}
	
	private static String resolveAwsBucketObjectKey(ResourceBundle resource) {
		
		String bucket = System.getenv("APP_AWS_BUCKET_OBJECT_KEY");
		if (bucket == null || bucket.isBlank()) {
			bucket = resource.getString("s3.bucket.object.name");
		}
		
		return bucket;

	}
	
	private static String resolveAwsOutBucketName(ResourceBundle resource) {
		
		String bucket = System.getenv("APP_AWS_OUT_BUCKET_NAME");
		if (bucket == null || bucket.isBlank()) {
			String key = "s3.outbound.bucket.name";
			if (resource.containsKey(key)) {
				bucket = resource.getString("s3.outbound.bucket.name");
			}
		}

		return bucket;

	}
	
	private static AwsS3Client newS3ClientInstance() {
		String awsKey = System.getenv("APP_AWS_KEY");
		String awsSecret = System.getenv("APP_AWS_SECRET");
		String awsRegion = System.getenv("APP_AWS_REGION");
		Region region = Region.of(awsRegion);
		AwsS3Client s3 = null;
		if (awsKey != null && awsSecret != null) {
			System.out.printf("Creating S3 Client: Found ENV Credentials (APP_AWS_KEY, APP_AWS_SECRET). %n");
			AwsCredentials credentials = AwsBasicCredentials.create(awsKey, awsSecret); 
			AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);
			s3 = new AwsS3Client(credentialsProvider, region);
		} else {
			System.out.printf("Creating S3 Client: Using Container Credentials. %n");
			s3 = new AwsS3Client(region);
		}
		return s3;
	}

}
