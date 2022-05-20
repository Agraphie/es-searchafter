package com.github.agraphie.elasticsearch;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Arrays;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

class SearchAfterSpliterator implements Spliterator<SearchHit> {
  private final String[] indices;
  private final ESClient esClient;
  private final SearchSourceBuilder searchSourceBuilder;
  private final AtomicInteger currentIndex = new AtomicInteger(0);
  private final ReentrantLock reentrantLock;
  private volatile Object[] lastSortValues;
  private SearchHits currentHits;

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
  }

  private SearchAfterSpliterator(
      SearchSourceBuilder searchRequestBuilder,
      ESClient esClient,
      SearchHits currentHits,
      Object[] lastSortValues,
      ReentrantLock reentrantLock,
      String... indices) {
    if (searchRequestBuilder.sorts() == null || searchRequestBuilder.sorts().isEmpty()) {
      throw new IllegalArgumentException(
          "Need to set at least one sort field for search after searches!");
    }
    this.indices = indices.clone();
    this.searchSourceBuilder = searchRequestBuilder;
    this.esClient = esClient;
    this.currentHits = currentHits;
    this.lastSortValues = lastSortValues;
    this.reentrantLock = reentrantLock;
  }

  @Override
  public boolean tryAdvance(Consumer<? super SearchHit> action) {
    if (currentHits == null) {
      getNextPage();
    }

    action.accept(currentHits.getAt(currentIndex.getAndIncrement()));
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
    int splitPos = size / 2 + currentIndex.get();
    SearchHit[] splitHits = Arrays.copyOfRange(hits, currentIndex.get(), splitPos);
    if (splitHits.length == 0) {
      return null;
    }

    currentIndex.set(splitPos);
    SearchHits newSpliterator =
        new SearchHits(splitHits, currentHits.getTotalHits(), currentHits.getMaxScore());

    return new SearchAfterSpliterator(
        searchSourceBuilder, esClient, newSpliterator, lastSortValues, reentrantLock, indices);
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
        && (currentHits.getHits().length > 0 && currentIndex.get() < currentHits.getHits().length);
  }

  private void getNextPageIfAtEndOfCurrent() {
    if (currentHits != null && currentIndex.get() < currentHits.getHits().length) {
      return;
    }

    getNextPage();
  }

  private void getNextPage() {
    if (!reentrantLock.tryLock()) {
      return;
    }

    if (lastSortValues != null) {
      searchSourceBuilder.searchAfter(lastSortValues);
    }

    SearchRequest searchRequest = new SearchRequest(indices);
    searchRequest.source(searchSourceBuilder);
    SearchResponse currentResponse = esClient.search(searchRequest);
    currentHits = currentResponse.getHits();
    if (currentHits != null && currentHits.getHits().length > 0) {
      SearchHit lastHit = currentHits.getAt(currentHits.getHits().length - 1);
      lastSortValues = lastHit.getSortValues();
    }
    reentrantLock.unlock();
    currentIndex.set(0);
  }
}
