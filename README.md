# Elasticsearch SearchAfter
This repo is an example of how to implement an iterator and a parallel-safe spliterator for the
SearchAfter[^1] operation in Elasticsearch.

## Why?
As scrolling and deep pagination is discouraged[^2], these iterators provider a more performant way.


## Usage
To use them, copy and paste the needed classes into your project. You might have to adjust
the imports.

You need to specify a sort value as defined by the Elasticsearch documentation!

## Examples
### Iterator
```JAVA
SearchAfterIterable searchAfterIterable =
  new SearchAfterIterable(
      searchSourceBuilder,
      searchRequest -> {
        try {
          return client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      },
      "mydocuments");

for (SearchHit documentFields : searchAfterIterable) {
System.out.println(documentFields);
}
```

### Spliterator
```JAVA
SearchSourceBuilder searchSourceBuilder =
  new SearchSourceBuilder().size(5000).query(QueryBuilders.matchAllQuery()).sort("_id");
SearchAfterSpliterator searchAfterSpliterator =
  new SearchAfterSpliterator(
      searchSourceBuilder,
      searchRequest -> {
        try {
          return client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      },
      "mydocuments");

StreamSupport.stream(searchAfterSpliterator, true).forEach(System.out::println);
```
[^1]: https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#search-after
[^2]: https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html