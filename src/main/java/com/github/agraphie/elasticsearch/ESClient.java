package com.github.agraphie.elasticsearch;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;

@FunctionalInterface
public interface ESClient {
    SearchResponse search(SearchRequest searchRequest);
}
