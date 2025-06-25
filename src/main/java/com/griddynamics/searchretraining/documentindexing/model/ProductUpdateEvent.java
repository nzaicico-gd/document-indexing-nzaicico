package com.griddynamics.searchretraining.documentindexing.model;

import java.time.Instant;

public record ProductUpdateEvent(
        String id,
        String field,
        String oldValue,
        String newValue,
        Instant timestamp
) {
}
