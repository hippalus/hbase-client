package com.github.hippalus.hbase.common;

import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;

public interface HBaseQueryOperations extends HBaseOperations {

  <T> T find(TableName tableName, String family, final ResultsExtractor<T> action) throws Exception;

  <T> T find(TableName tableName, FamilyAndColumn familyAndColumn, final ResultsExtractor<T> action) throws Exception;


  <T> T find(TableName tableName, final Scan scan, final ResultsExtractor<T> action) throws Exception;


  <T> List<T> find(TableName tableName, String family, final RowMapper<T> action) throws Exception;


  <T> List<T> find(TableName tableName, FamilyAndColumn familyAndColumn, final RowMapper<T> action) throws Exception;


  <T> List<T> find(TableName tableName, final Scan scan, final RowMapper<T> action) throws Exception;

  <T> List<T> get(TableName tableName, List<String> rowKeys, final RowMapper<T> action);

  <T> Optional<T> get(TableName tableName, String rowKey, final RowMapper<T> mapper);

  <T> T get(TableName tableName, RangeQueryParameters rangeParam, final ResultsExtractor<T> action);

  <T> Optional<T> get(TableName tableName, String rowKey, final FamilyAndColumn familyAndColumn, final RowMapper<T> mapper);
}

