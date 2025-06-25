package com.griddynamics.searchretraining.documentindexing.integration;

import com.griddynamics.searchretraining.documentindexing.model.ProductUpdateEvent;
import com.griddynamics.searchretraining.documentindexing.repository.ProductIndexerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.integration.core.GenericHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class ProductUpdatedEventHandler implements GenericHandler<List<ProductUpdateEvent>> {

    private final ProductIndexerRepository productIndexerRepository;

    @Override
    public Object handle(List<ProductUpdateEvent> products, MessageHeaders headers) {
        productIndexerRepository.updateProducts(products);
        return null;
    }
}