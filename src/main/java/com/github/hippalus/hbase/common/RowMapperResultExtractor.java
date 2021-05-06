package com.github.hippalus.hbase.common;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

public final class RowMapperResultExtractor<T> implements ResultsExtractor<List<T>> {

  private final RowMapper<T> rowMapper;

  public RowMapperResultExtractor(RowMapper<T> rowMapper) {
    Preconditions.checkNotNull(rowMapper, "RowMapper is required!");
    this.rowMapper = rowMapper;
  }

  @Override
  public List<T> extractData(ResultScanner results) {
    final List<T> resultList = new ArrayList<>();
    int rowNum = 0;
    for (Result result : results) {
      this.rowMapper.mapRow(result, rowNum++).ifPresent(resultList::add);
    }
    return resultList;
  }
}
