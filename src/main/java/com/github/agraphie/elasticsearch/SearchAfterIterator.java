package com.github.agraphie.elasticsearch;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Iterator;
import java.util.NoSuchElementException;

class SearchAfterIterator implements Iterator<SearchHit> {
  private final String[] indices;
  private final ESClient esClient;
  private final SearchSourceBuilder searchSourceBuilder;
  private SearchResponse currentResponse;
  private int currentIndex = 0;

  SearchAfterIterator(
      SearchSourceBuilder searchRequestBuilder, ESClient es7Client, String... indices) {
    if (searchRequestBuilder.sorts() == null || searchRequestBuilder.sorts().isEmpty()) {
      throw new IllegalArgumentException(
          "Need to set at least one sort field for search after searches!");
    }
    this.indices = indices.clone();
    this.searchSourceBuilder = searchRequestBuilder;
    this.esClient = es7Client;
  }

  @Override
  public boolean hasNext() throws ESException {
    if (currentResponse == null
        || currentResponse.getHits() != null
            && currentIndex >= currentResponse.getHits().getHits().length) {
      if (currentResponse != null && currentResponse.getHits() != null) {
        int position = currentResponse.getHits().getHits().length - 1;
        if (position < 0) {
          return false;
        }

        SearchHit lastHit = currentResponse.getHits().getAt(position);
        searchSourceBuilder.searchAfter(lastHit.getSortValues());
      }

      SearchRequest searchRequest = new SearchRequest(indices);
      searchRequest.source(searchSourceBuilder);
      currentResponse = esClient.search(searchRequest);
      currentIndex = 0;
    }

    return currentResponse.getHits() != null && currentResponse.getHits().getHits().length > 0;
  }

  @Override
  public SearchHit next() {
    if (currentIndex >= currentResponse.getHits().getHits().length) {
      throw new NoSuchElementException();
    }

    return currentResponse.getHits().getAt(currentIndex++);
  }
}
