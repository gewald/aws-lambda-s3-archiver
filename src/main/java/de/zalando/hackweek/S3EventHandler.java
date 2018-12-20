package de.zalando.hackweek;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import au.com.bytecode.opencsv.CSVReader;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.Lists;
import org.springframework.cloud.function.adapter.aws.SpringBootRequestHandler;
import org.springframework.util.StringUtils;

public class S3EventHandler extends SpringBootRequestHandler<S3Event, Context> {

    private static final String DEST_BUCKET = "repartitioned-events-bucket";
    private static final char SEPARATOR = ',';

    private static final int SIMPLE_SKU_LENGTH = 20; // Probably legacy sku have a different length
    private static final int MODEL_SKU_LENGTH = 9;


    public S3EventHandler(Class<?> configurationClass) {
        super(configurationClass);
    }

    @Override
    public Object handleRequest(S3Event event, Context context) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        LambdaLogger logger = context.getLogger();
        Map<String, List<S3Object>> repartitionedEventFiles = new HashMap<>();

        for (S3EventNotificationRecord record : event.getRecords()) {
            try {
                String srcBucket = record.getS3().getBucket().getName();
                String srcKey = record.getS3().getObject().getKey();
                S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));

                repartitionEventFiles(s3Object, repartitionedEventFiles);

                uploadToS3(s3Client, logger, srcBucket, repartitionedEventFiles);

            } catch (Exception ex) {
                // TODO errorhandling should be improved!!!
                logger.log("Something happened while repartitioning...");
            }
        }

        return super.handleRequest(event, context);
    }

    private void uploadToS3(AmazonS3 s3Client,
                            LambdaLogger logger,
                            String srcBucket,
                            Map<String, List<S3Object>> files) {

        for (String modelSku : files.keySet()) {
            for (S3Object eventFile : files.get(modelSku)) {
                s3Client.copyObject(srcBucket, eventFile.getKey(), DEST_BUCKET, modelSku + "/" + eventFile.getKey());
            }
            logger.log(String.format("Successfully repartitioned %s and uploaded to %s/s%.", srcBucket, DEST_BUCKET, modelSku));
        }

        logger.log(String.format("%s event files repartitioned.", files.size()));
    }

    private void repartitionEventFiles(S3Object s3Object, Map<String, List<S3Object>> repartitionedEventFiles) throws
            IOException {
        CSVReader reader = new CSVReader(
                new BufferedReader(new InputStreamReader(s3Object.getObjectContent())), SEPARATOR);

        String[] lineRecord;

        while ((lineRecord = reader.readNext()) != null) {
            final String modelSkuString = toModelSkuString(lineRecord[1]);
            if (repartitionedEventFiles.get(modelSkuString) != null) {
                repartitionedEventFiles.get(modelSkuString).add(s3Object);
            } else {
                repartitionedEventFiles.put(modelSkuString, Lists.newArrayList(s3Object));
            }
        }
    }

    private static String toModelSkuString(String simpleSku) {
        if (!StringUtils.isEmpty(simpleSku) && simpleSku.length() == SIMPLE_SKU_LENGTH) {
            return simpleSku.substring(0, MODEL_SKU_LENGTH);
        }

        throw new IllegalArgumentException(String.format("Given simple-sku [%s] has no Simple-Sku semantic", simpleSku));
    }

}