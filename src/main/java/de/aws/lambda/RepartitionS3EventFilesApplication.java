package de.aws.lambda;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RepartitionS3EventFilesApplication {

    public static void main(String[]  args) {
        SpringApplication.run(RepartitionS3EventFilesApplication.class, args);
    }
}
