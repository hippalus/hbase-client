package com.github.hippalus.hbase.common;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

@Slf4j
public class HBaseQueryOperationsImpl implements HBaseQueryOperations {

  private final Connection readConnection;

  public HBaseQueryOperationsImpl(Connection connection) {
    this.readConnection = connection;
  }

  @Override
  public Connection getConnection() {
    return this.readConnection;
  }

  @Override
  public void warmUpConnectionCache(TableName tableName) throws IOException {
    try (RegionLocator locator = readConnection.getRegionLocator(tableName)) {
      log.info("Warmed up region location cache for {} got {} ", tableName, locator.getAllRegionLocations().size());
    }
  }

  @Override
  public <T> T find(TableName tableName, String family, final ResultsExtractor<T> action) throws Exception {
    Scan scan = new Scan();
    scan.addFamily(toBytes(family));
    return find(tableName, scan, action);
  }

  @Override
  public <T> T find(TableName tableName, FamilyAndColumn familyAndColumn, final ResultsExtractor<T> action) throws Exception {
    Scan scan = new Scan();
    scan.addColumn(familyAndColumn.getFamilyBytes(), familyAndColumn.getColumnBytes());
    return find(tableName, scan, action);
  }

  @Override
  public <T> T find(TableName tableName, final Scan scan, final ResultsExtractor<T> action) throws Exception {
    try (Table table = readConnection.getTable(tableName)) {
      try (ResultScanner resultScanner = table.getScanner(scan)) {
        return action.extractData(resultScanner);
      }
    }
  }

  @Override
  public <T> List<T> find(TableName tableName, String family, final RowMapper<T> action) throws Exception {
    Scan scan = new Scan();
    scan.addFamily(toBytes(family));
    return this.find(tableName, scan, action);
  }

  @Override
  public <T> List<T> find(TableName tableName, FamilyAndColumn familyAndColumn, final RowMapper<T> action) throws Exception {
    Scan scan = new Scan();
    scan.setCaching(5000);
    scan.addColumn(familyAndColumn.getFamilyBytes(), familyAndColumn.getColumnBytes());
    return this.find(tableName, scan, action);
  }

  @Override
  public <T> List<T> find(TableName tableName, final Scan scan, final RowMapper<T> action) throws Exception {
    try (Table table = readConnection.getTable(tableName)) {
      if (scan.getCaching() < 50) {//TODO : Configure this value
        scan.setCaching(5000); // Check GC problems
      }
      scan.setCacheBlocks(false);
      try (ResultScanner resultScanner = table.getScanner(scan)) {
        List<T> resultList = new ArrayList<>();
        int rowNum = 0;
        for (Result result : resultScanner) {
          action.mapRow(result, rowNum++).ifPresent(resultList::add);
        }
        return resultList;
      }
    }
  }

  @SneakyThrows
  @Override
  public <T> List<T> get(TableName tableName, List<String> rowKeys, final RowMapper<T> action) {
    List<Get> gets = new ArrayList<>(rowKeys.size());
    for (String rowKey : rowKeys) {
      gets.add(new Get(toBytes(rowKey)));
    }
    List<T> records = new ArrayList<>(rowKeys.size());
    try (Table table = readConnection.getTable(tableName)) {
      Result[] results = table.get(gets);
      int rowCount = 0;
      for (Result result : results) {
        action.mapRow(result, rowCount++).ifPresent(records::add);
      }
    }
    return records;
  }

  @SneakyThrows
  @Override
  public <T> T get(TableName tableName, RangeQueryParameters rangeParam, final ResultsExtractor<T> action) {
    Scan scan = new Scan()
        .withStartRow(rangeParam.getStartRowKeyBytes(), rangeParam.isStartRowInclusive())
        .withStopRow(rangeParam.getEndRowKeyBytes(), rangeParam.isEndRowInclusive());
    try (Table table = readConnection.getTable(tableName); ResultScanner scanner = table.getScanner(scan)) {
      return action.extractData(scanner);
    }
  }

  @Override
  public <T> Optional<T> get(TableName tableName, String rowKey, final RowMapper<T> mapper) {
    return get(tableName, rowKey, null, mapper);
  }

  @SneakyThrows
  @Override
  public <T> Optional<T> get(TableName tableName, String rowKey, FamilyAndColumn familyAndColumn, final RowMapper<T> mapper) {
    try (Table table = readConnection.getTable(tableName)) {
      Get get = new Get(toBytes(rowKey));
      if (familyAndColumn != null) {
        byte[] family = familyAndColumn.getFamilyBytes();
        if (familyAndColumn.columnIsNonNull()) {
          get.addColumn(family, familyAndColumn.getColumnBytes());
        } else {
          get.addFamily(family);
        }
      }
      Result result = table.get(get);
      return mapper.mapRow(result, 0);
    }
  }

}
