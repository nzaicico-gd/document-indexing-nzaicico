package com.griddynamics.searchretraining.documentindexing.model;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

public record ProductDocument(
        String id,
        String name,
        String description,
        String category,
        String brand,
        Double price,
        String currency,
        Boolean available,
        Integer stock,
        List<String> color,
        List<String> size,
        ZonedDateTime lastUpdated
) {

    public static Set<String> updatableFields() {
        return Set.of(
                ProductDocument.Fields.NAME,
                ProductDocument.Fields.DESCRIPTION,
                ProductDocument.Fields.CATEGORY,
                ProductDocument.Fields.BRAND,
                ProductDocument.Fields.PRICE,
                ProductDocument.Fields.CURRENCY,
                ProductDocument.Fields.AVAILABLE,
                ProductDocument.Fields.STOCK,
                ProductDocument.Fields.COLOR,
                ProductDocument.Fields.SIZE
        );
    }

    public static final class Fields {
        public static final String ID = "id";
        public static final String NAME = "name";
        public static final String DESCRIPTION = "description";
        public static final String CATEGORY = "category";
        public static final String BRAND = "brand";
        public static final String PRICE = "price";
        public static final String CURRENCY = "currency";
        public static final String AVAILABLE = "available";
        public static final String STOCK = "stock";
        public static final String COLOR = "color";
        public static final String SIZE = "size";
        public static final String LAST_UPDATED = "lastUpdated";
    }
}