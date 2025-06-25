package com.griddynamics.searchretraining.documentindexing;

import com.griddynamics.searchretraining.documentindexing.config.IndexProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({IndexProperties.class})
public class DocumentIndexingApplication {

    public static void main(String[] args) {
        SpringApplication.run(DocumentIndexingApplication.class, args);
    }

}
