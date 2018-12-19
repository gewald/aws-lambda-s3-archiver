package de.zalando.hackweek;

import com.amazonaws.services.s3.AmazonS3;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class S3Archiver {

    @Value("${cloud.aws.s3_bucket_archive}")
    private String bucketNameArchive;

    @Value("${cloud.aws.s3_bucket_source}")
    private String bucketNameSource;

    private String sourceKey = "";

    @Autowired
    private AmazonS3 amazonS3Client;

    public void archiveEvent(LambdaHandler.ArticleModel articleModel) {



        amazonS3Client.copyObject(bucketNameSource, sourceKey, bucketNameArchive, articleModel.getModelSku());
    }

    private String constructArchivePath(final String modelSku, final String supplierArticleNumber,
                                      final String supplierColorCode, final String imageFilename) {
        return on('/').skipNulls().join(supplierCode, supplierArticleNumber, supplierColorCode, imageFilename);
    }


}