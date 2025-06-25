package com.griddynamics.searchretraining.documentindexing.model;

public record ProductIndexError(
        String productId,
        String reason
) {

}
