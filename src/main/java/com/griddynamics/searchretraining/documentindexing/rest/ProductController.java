package com.griddynamics.searchretraining.documentindexing.rest;

import com.griddynamics.searchretraining.documentindexing.model.ProductDocument;
import com.griddynamics.searchretraining.documentindexing.model.ProductUpdateEvent;
import com.griddynamics.searchretraining.documentindexing.model.UpdateCatalogResponse;
import com.griddynamics.searchretraining.documentindexing.repository.ProductIndexerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(value = "/document-indexing/products")
@RequiredArgsConstructor
public class ProductController {

    private final ProductIndexerRepository productIndexerRepository;

    private final MessageChannel productUpdateChannel;

    @PostMapping("/index")
    public UpdateCatalogResponse reindexCatalog() {
        return productIndexerRepository.reindexCatalog();
    }

    @PutMapping("/asyncUpdate")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void bulkUpdateProducts(@RequestBody List<ProductUpdateEvent> productUpdateEvents) {
        productUpdateEvents.forEach(event ->
                productUpdateChannel.send(MessageBuilder.withPayload(event).build()));
    }

    @GetMapping("/{productId}")
    public ProductDocument bulkUpdateProducts(@PathVariable String productId) {
        return productIndexerRepository.getProduct(productId).source();
    }


}
