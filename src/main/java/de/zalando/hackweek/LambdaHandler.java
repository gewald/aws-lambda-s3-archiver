package de.zalando.hackweek;

import java.util.Set;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;

import static de.zalando.hackweek.ModelConfigSimpleConverter.toArticleModel;

public class LambdaHandler implements RequestHandler<LogEntry, Void> {

    @Autowired
    private S3Archiver s3Archiver;

    public Void handleRequest(LogEntry logEntry, Context context) {

        final ArticleModel articleModel = toArticleModel(logEntry);

        s3Archiver.archiveEvent(articleModel);

        return null;
    }

    @Data
    class ArticleModel {
        private String modelSku;
        private Set<ArticleConfig> children;
    }

    @Data
    class ArticleConfig {
        private String configSku;
        private Set<ArticleSimple> children;
    }

    @Data
    class ArticleSimple {
        private String simpleSku;
        private LogEntry logEntry;
    }

}