package com.griddynamics.searchretraining.documentindexing;

import com.griddynamics.searchretraining.documentindexing.model.ProductDocument;
import com.griddynamics.searchretraining.documentindexing.model.ProductUpdateEvent;
import com.griddynamics.searchretraining.documentindexing.repository.ProductIndexerRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
public class ProductPipelineIntegrationTest {

    @Autowired
    private MessageChannel productUpdateChannel;

    @Autowired
    private ProductIndexerRepository productIndexerRepository;

    @BeforeEach
    void setup() {
        productIndexerRepository.reindexCatalog();
    }

    @Test
    void shouldUpdateEfficiently() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Instant fiveSecondsAgo = now.minusSeconds(5);
        Instant fourSecondsAgo = now.minusSeconds(4);

        String newDescription = "Cycling jersey with reflective accents";

        List<ProductUpdateEvent> events = List.of(
                // SKU-20001 difference check
                new ProductUpdateEvent("SKU-20001", "price", "89.99", "79.99", fiveSecondsAgo),
                new ProductUpdateEvent("SKU-20001", "price", "79.99", "79.99", fourSecondsAgo),

                // SKU-20002 merge consecutive events sorted by timestamp
                new ProductUpdateEvent("SKU-20002", "stock", "149", "148", fourSecondsAgo),
                new ProductUpdateEvent("SKU-20002", "stock", "150", "149", fiveSecondsAgo),

                // SKU-20003 description change
                new ProductUpdateEvent("SKU-20003", "description",
                        "Lightweight, moisture-wicking cycling jersey with reflective accents.",
                        newDescription, fiveSecondsAgo),

                // SKU-20004 skipped non-indexed field change
                new ProductUpdateEvent("SKU-20004", "internalNote", "note 1", " note 2", fiveSecondsAgo),

                // SKU-20005 merge consecutive events sorted in order
                new ProductUpdateEvent("SKU-20005", "price", "99.99", "89.99", fiveSecondsAgo),
                new ProductUpdateEvent("SKU-20005", "price", "89.99", "79.99", fiveSecondsAgo)
        );


        events.forEach(event -> productUpdateChannel.send(MessageBuilder.withPayload(event).build()));

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertProduct("SKU-20001", 2, p -> assertThat(p.price()).isEqualTo(79.99));
            assertProduct("SKU-20002", 2, p -> assertThat(p.stock()).isEqualTo(148));
            assertProduct("SKU-20003", 2, p -> assertThat(p.description()).isEqualTo(newDescription));
            assertProduct("SKU-20004", 1, p -> {});
            assertProduct("SKU-20005", 2, p -> assertThat(p.price()).isEqualTo(79.99));
        });
    }

    private void assertProduct(String sku, int expectedVersion, Consumer<ProductDocument> productAssertions) {
        var response = productIndexerRepository.getProduct(sku);
        assertThat(response.found()).isTrue();
        assertThat(response.version()).isEqualTo(expectedVersion);
        assertThat(response.source()).isNotNull();
        productAssertions.accept(response.source());
    }
}

