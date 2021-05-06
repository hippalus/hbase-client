package com.github.hippalus.hbase.common;

import org.apache.hadoop.hbase.client.ResultScanner;

public interface ResultsExtractor<T> {

  T extractData(ResultScanner results);
}
