package com.github.agraphie.elasticsearch;

import java.util.Arrays;
import java.util.Spliterator;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;

class SearchAfterSpliterator implements Spliterator<SearchHit> {

  private final String[] indices;
  private final ESClient esClient;
  private final SearchSourceBuilder searchSourceBuilder;
  private int currentIndex;
  private final ReentrantLock reentrantLock;
  private volatile Object[] lastSortValues;
  private SearchHits currentHits;
  private final PointInTimeBuilder pointInTimeBuilder;

  SearchAfterSpliterator(
      SearchSourceBuilder searchRequestBuilder, ESClient esClient, String... indices) {
    if (searchRequestBuilder.sorts() == null || searchRequestBuilder.sorts().isEmpty()) {
      throw new IllegalArgumentException(
          "Need to set at least one sort field for search after searches!");
    }
    this.indices = indices.clone();
    this.searchSourceBuilder = searchRequestBuilder;
    this.esClient = esClient;
    this.reentrantLock = new ReentrantLock();
    this.pointInTimeBuilder = null;
  }

  SearchAfterSpliterator(
      SearchSourceBuilder searchRequestBuilder,
      PointInTimeBuilder pointInTimeBuilder,
      ESClient esClient) {
    this.indices = null;
    this.searchSourceBuilder =
        searchRequestBuilder.shallowCopy().pointInTimeBuilder(pointInTimeBuilder);
    this.esClient = esClient;
    this.reentrantLock = new ReentrantLock();
    this.pointInTimeBuilder = pointInTimeBuilder;
  }

  private SearchAfterSpliterator(
      SearchSourceBuilder searchRequestBuilder,
      ESClient esClient,
      SearchHits currentHits,
      Object[] lastSortValues,
      ReentrantLock reentrantLock,
      PointInTimeBuilder pointInTimeBuilder,
      String... indices) {
    if (searchRequestBuilder.sorts() == null || searchRequestBuilder.sorts().isEmpty()) {
      throw new IllegalArgumentException(
          "Need to set at least one sort field for search after searches!");
    }
    if (indices != null) {
      this.indices = indices.clone();
    } else {
      this.indices = null;
    }
    this.searchSourceBuilder = searchRequestBuilder;
    this.esClient = esClient;
    this.currentHits = currentHits;
    this.lastSortValues = lastSortValues;
    this.reentrantLock = reentrantLock;
    this.pointInTimeBuilder = pointInTimeBuilder;
  }

  @Override
  public boolean tryAdvance(Consumer<? super SearchHit> action) {
    if (currentHits == null) {
      getNextPage();
    }

    action.accept(currentHits.getAt(currentIndex++));
    getNextPageIfAtEndOfCurrent();

    return elementsLeft();
  }

  @Override
  public Spliterator<SearchHit> trySplit() {
    getNextPageIfAtEndOfCurrent();
    if (currentHits == null || currentHits.getHits().length == 0) {
      return null;
    }
    SearchHit[] hits = currentHits.getHits();
    int size = hits.length;
    int splitPos = size / 2 + currentIndex;
    SearchHit[] splitHits = Arrays.copyOfRange(hits, currentIndex, splitPos);
    if (splitHits.length == 0) {
      return null;
    }

    currentIndex = splitPos;
    SearchHits newSpliterator =
        new SearchHits(splitHits, currentHits.getTotalHits(), currentHits.getMaxScore());

    return new SearchAfterSpliterator(
        searchSourceBuilder,
        esClient,
        newSpliterator,
        lastSortValues,
        reentrantLock,
        pointInTimeBuilder,
        indices);
  }

  @Override
  public long estimateSize() {
    if (currentHits != null && currentHits.getTotalHits().relation == TotalHits.Relation.EQUAL_TO) {
      return currentHits.getTotalHits().value;
    } else if (currentHits != null
        && currentHits.getHits().length < currentHits.getTotalHits().value) {
      return currentHits.getHits().length;
    }

    return Integer.MAX_VALUE;
  }

  @Override
  public int characteristics() {
    return IMMUTABLE;
  }

  private boolean elementsLeft() {
    return currentHits != null
        && (currentHits.getHits().length > 0 && currentIndex < currentHits.getHits().length);
  }

  private void getNextPageIfAtEndOfCurrent() {
    if (currentHits != null && currentIndex < currentHits.getHits().length) {
      return;
    }

    getNextPage();
  }

  private void getNextPage() {
    try {
      reentrantLock.lock();
      final SearchRequest searchRequest;
      if (indices == null || indices.length == 0) {
        searchRequest = new SearchRequest();
      } else {
        searchRequest = new SearchRequest(indices);
      }
      if (lastSortValues != null) {
        searchSourceBuilder.searchAfter(lastSortValues);
      }

      searchRequest.source(searchSourceBuilder);
      SearchResponse currentResponse = esClient.search(searchRequest);
      currentHits = currentResponse.getHits();
      if (currentHits != null && currentHits.getHits().length > 0) {
        SearchHit lastHit = currentHits.getAt(currentHits.getHits().length - 1);
        lastSortValues = lastHit.getSortValues();
      }
    } finally {
      reentrantLock.unlock();
    }
    currentIndex = 0;
  }
}
