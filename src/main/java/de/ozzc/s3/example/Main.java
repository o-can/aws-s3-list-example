package de.ozzc.s3.example;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Ozkan Can
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) {

        if (args.length < 1) {
            LOGGER.error("Start the program with a valid bucket name as argument.");
            System.exit(-1);
        }
        final String bucketName = args[0];
        String prefix = null;
        if (args.length == 2)
        {
            prefix = args[1];
        }

        AmazonS3Client s3Client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
        String region = s3Client.getBucketLocation(bucketName);
        s3Client.setRegion(Region.getRegion(Regions.fromName(region)));
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
        if(prefix != null)
        {
            LOGGER.info("Listing Objects using prefix: "+prefix);
            listObjectsRequest = listObjectsRequest.withPrefix(prefix);
        }
        ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
        LOGGER.info(objectListing.toString());
        List<S3ObjectSummary> summaries = objectListing.getObjectSummaries();

        LOGGER.info("Unsorted");
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            LOGGER.info(summary.getKey() + " LM: " + summary.getLastModified());

        }

        LOGGER.info("Sorted by Last Modified Date");
        List<S3ObjectSummary> sortedSummaries =
                summaries.stream()
                        .sorted((o1, o2) -> o1.getLastModified().compareTo(o2.getLastModified()))
                        .collect(Collectors.toList());
        for (S3ObjectSummary sortedSummary : sortedSummaries) {
            LOGGER.info(sortedSummary.getKey() + " LM: " + sortedSummary.getLastModified());

        }
        LOGGER.info("Oldest: " + sortedSummaries.get(0).getKey() + " LM: " + sortedSummaries.get(0).getLastModified());

        S3ObjectSummary newestSummary =
                summaries.stream()
                        .sorted((o1, o2) -> o2.getLastModified().compareTo(o1.getLastModified()))
                        .findFirst().get();
        LOGGER.info("Newest: " + newestSummary.getKey() + " LM: " + newestSummary.getLastModified());

        BucketWebsiteConfiguration websiteConfiguration = s3Client.getBucketWebsiteConfiguration(bucketName);
        if (websiteConfiguration != null) {
            LOGGER.info("Website URL: " + getWebsiteEndpoint(bucketName, Regions.fromName(region)) + "/" + newestSummary.getKey());
        }
    }


    public static String getWebsiteEndpoint(String bucketName, Regions region) {
        switch (region) {
            case US_EAST_1:
            case US_WEST_1:
            case US_WEST_2:
            case EU_WEST_1:
            case AP_SOUTHEAST_1:
            case AP_SOUTHEAST_2:
            case AP_NORTHEAST_1:
            case SA_EAST_1:
                return String.format("http://%s.s3-website-%s.amazonaws.com", bucketName, region.getName());
            case EU_CENTRAL_1:
            case AP_NORTHEAST_2:
                return String.format("http://%s.s3-website.%s.amazonaws.com", bucketName, region.getName());
            case CN_NORTH_1:
            case GovCloud:
                return "<UNDEFINED>";
        }
        return "";
    }
}
