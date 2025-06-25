package com.griddynamics.searchretraining.documentindexing.model;

import java.util.List;

public record UpdateCatalogResponse(
        Integer count,
        List<ProductIndexError> errors
) {

}
