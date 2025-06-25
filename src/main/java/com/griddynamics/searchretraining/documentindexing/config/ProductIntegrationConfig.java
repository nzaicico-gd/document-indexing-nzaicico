package com.griddynamics.searchretraining.documentindexing.config;

import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.griddynamics.searchretraining.documentindexing.integration.ProductUpdatedEventHandler;
import com.griddynamics.searchretraining.documentindexing.integration.ProductUpdatedEventTransformer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.PollableChannel;

@Configuration
public class ProductIntegrationConfig {

    @Bean
    public PollableChannel productUpdateChannel() {
        return new QueueChannel();
    }

    @Bean
    public IntegrationFlow productUpdateFlow(
            PollableChannel productUpdateChannel,
            ProductUpdatedEventTransformer transformer,
            ProductUpdatedEventHandler handler
    ) {
        return IntegrationFlow.from(productUpdateChannel)
                .aggregate(a -> a
                        .correlationStrategy(m -> "all")
                        .releaseStrategy(g -> g.size() >= 10)
                        .groupTimeout(1000)
                        .sendPartialResultOnExpiry(true)
                )
                .transform(transformer)
                .handle(handler)
                .get();
    }

    @Bean
    public JsonpMapper jsonpMapper(ObjectMapper objectMapper) {
        return new JacksonJsonpMapper(objectMapper);
    }
}