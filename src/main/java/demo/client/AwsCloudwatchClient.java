package demo.client;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

public class AwsCloudwatchClient {

	private CloudWatchClient  client;
	
	public AwsCloudwatchClient(Region region) {
		
		this.client = CloudWatchClient.builder()
				  //.credentialsProvider(credentialsProvider)
				  .region(region)
				  .build();
	}
	
	public AwsCloudwatchClient(AwsCredentialsProvider credentialsProvider, Region region) {
		
		this.client = CloudWatchClient.builder()
				  .credentialsProvider(credentialsProvider)
				  .region(region)
				  .build();
	}

	public void metricPut(String namespace, String metricName, Double metricValue, Map<String, String> dimensionMap) {
		
		System.out.println(String.format("Put CloudWatch Metric. Namespace=%s Name=%s Dimension=%s", namespace, metricName, dimensionMap));

		List<Dimension> dimensions = dimensionMap.entrySet().stream()
			.map(entry -> Dimension.builder().name(entry.getKey()).value(entry.getValue()).build())
			.collect(Collectors.toList());
		
		PutMetricDataRequest request = PutMetricDataRequest.builder()
				.namespace(namespace)
    			.metricData(MetricDatum.builder()
    					.metricName(metricName)
    					.unit(StandardUnit.COUNT)
    					.value(metricValue)
    					.dimensions(dimensions)
    					.timestamp(Instant.now().truncatedTo(ChronoUnit.MILLIS))
    					.build())
    			.build();

		PutMetricDataResponse result = client.putMetricData(request);
		System.out.println(result);
        
	}
	
}
