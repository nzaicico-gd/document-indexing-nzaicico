package com.griddynamics.searchretraining.documentindexing.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesRequest;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesResponse;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.griddynamics.searchretraining.documentindexing.config.IndexProperties;
import com.griddynamics.searchretraining.documentindexing.model.ProductDocument;
import com.griddynamics.searchretraining.documentindexing.model.ProductUpdateEvent;
import com.griddynamics.searchretraining.documentindexing.model.ProductIndexError;
import com.griddynamics.searchretraining.documentindexing.model.UpdateCatalogResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.griddynamics.searchretraining.documentindexing.model.ProductDocument.Fields.LAST_UPDATED;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ProductIndexerRepository {

    private static final int MAX_INDEXES_COUNT = 3;

    private final IndexProperties indexProperties;

    private final ElasticsearchClient esClient;

    private final ObjectMapper objectMapper;

    private final ResourceLoader resourceLoader;

    public UpdateCatalogResponse reindexCatalog() {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        String indexName = indexProperties.alias() + "_" + timestamp;

        if (indexExists(indexName)) {
            deleteIndex(indexName);
        }
        createNewIndex(indexName);
        UpdateCatalogResponse updateCatalogResponse = indexBulkData(indexName);
        assignAlias(indexName, indexProperties.alias());
        deleteOldIndexesIfRequired();

        return updateCatalogResponse;
    }

    public void updateProducts(List<ProductUpdateEvent> productUpdateEvents) {
        var bulkRequestBuilder = new BulkRequest.Builder()
                .refresh(Refresh.True);

        for (ProductUpdateEvent updateEvent : productUpdateEvents) {
            bulkRequestBuilder.operations(op -> op
                    .update(u -> u
                            .index(indexProperties.alias())
                            .id(updateEvent.id())
                            .action(a -> a
                                    .doc(Map.of(
                                            updateEvent.field(), updateEvent.newValue(),
                                            LAST_UPDATED, updateEvent.timestamp()
                                    ))
                            )
                    )
            );
        }

        try {
            BulkResponse result = esClient.bulk(bulkRequestBuilder.build());
            log.info("Documents have been updated: {}", result.items().size());
            if (result.errors()) {
                result.items().stream()
                        .filter(item -> item.error() != null)
                        .forEach(item -> log.warn("Error updating doc Id: {}, Error: {}", item.id(), item.error().reason()));
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to execute bulk update", e);
        }
    }

    public GetResponse<ProductDocument> getProduct(String id) {
        try {
            return esClient.get(g -> g.index(indexProperties.alias()).id(id), ProductDocument.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get document from index with id: " + id, e);
        }
    }

    private void createNewIndex(String indexName) {
        try (var settings = getResource(indexProperties.settings())) {
            CreateIndexRequest request = CreateIndexRequest.of(b -> b
                    .index(indexName)
                    .withJson(settings));

            CreateIndexResponse response = esClient.indices().create(request);

            if (!response.acknowledged()) {
                throw new IllegalStateException("Index creation not acknowledged for index: " + indexName);
            }

            log.info("Index '{}' has been created successfully.", indexName);
        } catch (IOException e) {
            throw new RuntimeException("An error occurred during index creation: " + indexName, e);
        }
    }

    private boolean indexExists(String indexName) {
        try {
            return esClient.indices().exists(e -> e.index(indexName)).value();
        } catch (Exception e) {
            throw new RuntimeException("An exception occurred while checking for the index existence: " + indexName, e);
        }
    }

    private UpdateCatalogResponse indexBulkData(String indexName) {
        List<Map<String, Object>> documents = readJsonResource(indexProperties.bulkData(), new TypeReference<>() {
        });

        var bulkRequestBuilder = new BulkRequest.Builder()
                .refresh(Refresh.True);

        documents.forEach(doc -> {
            Object id = doc.get("id");
            if (id == null) {
                log.warn("Skipping doc without 'id': {}", doc);
                return;
            }

            bulkRequestBuilder.operations(BulkOperation.of(b -> b
                    .index(i -> i
                            .index(indexName)
                            .id(id.toString())
                            .document(doc)
                    )));
        });

        try {
            BulkResponse response = esClient.bulk(bulkRequestBuilder.build());
            int itemsCount = response.items().size();
            List<ProductIndexError> errors = new ArrayList<>();

            if (response.errors()) {
                response.items().stream()
                        .filter(item -> item.error() != null)
                        .map(item -> new ProductIndexError(item.id(), item.error().reason()))
                        .forEach(errors::add);
            }

            return new UpdateCatalogResponse(itemsCount, errors);
        } catch (IOException e) {
            log.error("An exception occurred during bulk bulkData processing", e);
            throw new RuntimeException(e);
        }
    }

    private void assignAlias(String indexName, String alias) {
        List<Action> actions = new ArrayList<>();

        actions.add(Action.of(action -> action
                .remove(remove -> remove
                        .alias(alias)
                        .index(indexProperties.aliasPattern()))));

        actions.add(Action.of(action -> action
                .add(add -> add
                        .index(indexName)
                        .alias(alias))));

        try {
            UpdateAliasesResponse response = esClient.indices().updateAliases(UpdateAliasesRequest.of(r -> r
                    .actions(actions)));

            if (!response.acknowledged()) {
                log.warn("Alias assigment is not acknowledged for new index: {}", indexName);
            } else {
                log.info("Alias {} has been assigned to index {}.", alias, indexName);
            }

        } catch (IOException e) {
            throw new RuntimeException("Alias assigment is failed for index " + indexName, e);
        }
    }

    private void deleteOldIndexesIfRequired() {
        try {
            var getResponse = esClient.indices().get(g -> g
                    .index(indexProperties.aliasPattern()));

            Set<String> existingIndices = getResponse.result().keySet();

            if (existingIndices.size() > MAX_INDEXES_COUNT) {
                existingIndices.stream()
                        .sorted()
                        .limit(existingIndices.size() - MAX_INDEXES_COUNT)
                        .forEach(this::deleteIndex);
            }
        } catch (IOException e) {
            throw new RuntimeException("Index search failed for pattern " + indexProperties.aliasPattern(), e);
        }
    }

    private void deleteIndex(String indexName) {
        try {
            var deleteResponse = esClient.indices().delete(d -> d.index(indexName));

            if (!deleteResponse.acknowledged()) {
                log.warn("Index deletion not acknowledged for index: {}", indexName);
            } else {
                log.info("Index {} has been deleted.", indexName);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete index: " + indexName, e);
        }
    }

    private <T> T readJsonResource(String path, TypeReference<T> typeReference) {
        try (InputStream inputStream = getResource(path)) {
            return objectMapper.readValue(inputStream, typeReference);
        } catch (IOException e) {
            throw new IllegalArgumentException("Can not read resource file: " + path, e);
        }
    }

    public InputStream getResource(String resourcePath) throws IOException {
        Resource resource = resourceLoader.getResource(resourcePath);
        if (!resource.exists()) {
            throw new IllegalArgumentException("Resource not found: " + resource.getFilename());
        }
        return resource.getInputStream();
    }


}
