package com.griddynamics.searchretraining.documentindexing.integration;

import com.griddynamics.searchretraining.documentindexing.model.ProductDocument;
import com.griddynamics.searchretraining.documentindexing.model.ProductUpdateEvent;
import org.springframework.integration.core.GenericTransformer;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class ProductUpdatedEventTransformer implements GenericTransformer<List<ProductUpdateEvent>, List<ProductUpdateEvent>> {

    record Key(String id, String field) {
    }

    @Override
    public List<ProductUpdateEvent> transform(List<ProductUpdateEvent> productUpdateEvents) {
        return productUpdateEvents.stream()
                .filter(this::isIndexedField)
                .filter(this::valueChanged)
                .collect(Collectors.toMap(
                        e -> new Key(e.id(), e.field()),
                        Function.identity(),
                        this::mergeConsecutiveEvents
                ))
                .values().stream()
                .toList();
    }

    private boolean isIndexedField(ProductUpdateEvent productUpdateEvent) {
        return ProductDocument.updatableFields().contains(productUpdateEvent.field());
    }

    private boolean valueChanged(ProductUpdateEvent productUpdateEvent) {
        return !productUpdateEvent.newValue().equals(productUpdateEvent.oldValue());
    }

    private ProductUpdateEvent mergeConsecutiveEvents(ProductUpdateEvent existing, ProductUpdateEvent replacement) {
        return existing.timestamp().isAfter(replacement.timestamp())
                ? existing
                : replacement;
    }

}
