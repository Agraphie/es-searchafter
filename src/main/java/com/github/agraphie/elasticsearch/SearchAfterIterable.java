package com.github.agraphie.elasticsearch;

import java.util.Iterator;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;


public class SearchAfterIterable implements Iterable<SearchHit> {
  private final String[] indices;
  private final SearchSourceBuilder searchRequestBuilder;
  private final ESClient es7Client;

  public SearchAfterIterable(SearchSourceBuilder searchRequestBuilder, ESClient es7Client, String... indices) {
    this.searchRequestBuilder = searchRequestBuilder;
    this.es7Client = es7Client;
    this.indices = indices.clone();
  }

  @Override
  public Iterator<SearchHit> iterator() {
    return new SearchAfterIterator(searchRequestBuilder, es7Client, indices);
  }
}
