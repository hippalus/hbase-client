package com.github.hippalus.hbase.common;

import static com.github.hippalus.hbase.testutil.object.TestObjects.ANCHOR_FAMILY_AND_CRAWL_TIME_COLUMN;
import static com.github.hippalus.hbase.testutil.object.TestObjects.ANCHOR_FAMILY_AND_DOMAIN_NAME_COLUMN;
import static com.github.hippalus.hbase.testutil.object.TestObjects.ANCHOR_FAMILY_AND_URL_COLUMN;
import static com.github.hippalus.hbase.testutil.object.TestObjects.ANCHOR_FAMILY_AND_URL_FINGERPRINT_COLUMN;
import static com.github.hippalus.hbase.testutil.object.TestObjects.CONTENTS_FAMILY_AND_COLUMN;
import static com.github.hippalus.hbase.testutil.object.TestObjects.HTML_CONTENT_VALUE1;
import static com.github.hippalus.hbase.testutil.object.TestObjects.HTML_CONTENT_VALUE2;
import static com.github.hippalus.hbase.testutil.object.TestObjects.HTML_DUMMY2;
import static com.github.hippalus.hbase.testutil.object.TestObjects.TABLE_NAME;
import static com.github.hippalus.hbase.testutil.object.TestObjects.TABLE_NAME_FAMILY_LIST_MAP;
import static com.github.hippalus.hbase.util.SerializationUtils.deserialize;
import static com.github.hippalus.hbase.util.SerializationUtils.serialize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.hippalus.hbase.common.factory.HBaseCommandOperationsFactory;
import com.github.hippalus.hbase.common.factory.HBaseQueryOperationsFactory;
import com.github.hippalus.hbase.testutil.cluster.MiniCluster;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.ToString;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class HBaseOperationsImplTest {

  static final MiniCluster CLUSTER = MiniCluster.startup();

  static final HBaseQueryOperations queryOperations = getQueryOp();
  static final HBaseCommandOperations cmdOperations = getCommandOp();

  static HBaseQueryOperations getQueryOp() {
    Iterator<HBaseQueryOperationsFactory> factories = ServiceLoader.load(HBaseQueryOperationsFactory.class).iterator();
    if (!factories.hasNext()) {
      fail("No HBaseQueryOperationsFactory found");
    }
    try (HBaseQueryOperationsFactory factory = factories.next();
        HBaseQueryOperations queryOperations = factory.create(CLUSTER.getConnection())) {
      return queryOperations;
    } catch (Exception e) {
      fail(e.getMessage());
      return null;
    }
  }

  static HBaseCommandOperations getCommandOp() {
    Iterator<HBaseCommandOperationsFactory> factories = ServiceLoader.load(HBaseCommandOperationsFactory.class).iterator();
    if (!factories.hasNext()) {
      fail("No HBaseCommandOperationsFactory found");
    }
    try (HBaseCommandOperationsFactory factory = factories.next();
        HBaseCommandOperations commandOperations = factory.create(CLUSTER.getConnection())) {
      return commandOperations;
    } catch (Exception e) {
      fail(e.getMessage());
      return null;
    }
  }

  @BeforeAll
  static void beforeAll() {
    try {
      CLUSTER.createTables(TABLE_NAME_FAMILY_LIST_MAP);
      //----- ALL GIVEN DATA -----
      Map<String, byte[]> htmlData = new HashMap<>();
      htmlData.put("fingerPrint1", HTML_CONTENT_VALUE1);
      htmlData.put("fingerPrint2", HTML_CONTENT_VALUE2);
      Map<String, byte[]> domainNameData = new HashMap<>();
      domainNameData.put("fingerPrint1", Bytes.toBytes("com.trthaber.www"));
      domainNameData.put("fingerPrint2", Bytes.toBytes("com.cnn.www"));

      Map<String, byte[]> urlData = new HashMap<>();
      urlData.put("fingerPrint1", Bytes.toBytes("https://www.trthaber.com/"));
      urlData.put("fingerPrint2", Bytes.toBytes("https://edition.cnn.com/"));

      Map<String, byte[]> crawlTimeData = new HashMap<>();
      crawlTimeData.put("fingerPrint1", org.apache.commons.lang.SerializationUtils.serialize(Instant.now()));
      crawlTimeData.put("fingerPrint2", org.apache.commons.lang.SerializationUtils.serialize(Instant.now()));

      Map<String, byte[]> fingerPrint = new HashMap<>();
      fingerPrint.put("fingerPrint1", Bytes.toBytes("fingerPrint1"));
      fingerPrint.put("fingerPrint2", Bytes.toBytes("fingerPrint1"));

      cmdOperations.put(TABLE_NAME, CONTENTS_FAMILY_AND_COLUMN, htmlData);
      cmdOperations.put(TABLE_NAME, ANCHOR_FAMILY_AND_DOMAIN_NAME_COLUMN, domainNameData);
      cmdOperations.put(TABLE_NAME, ANCHOR_FAMILY_AND_URL_COLUMN, urlData);
      cmdOperations.put(TABLE_NAME, ANCHOR_FAMILY_AND_CRAWL_TIME_COLUMN, crawlTimeData);
      cmdOperations.put(TABLE_NAME, ANCHOR_FAMILY_AND_URL_FINGERPRINT_COLUMN, fingerPrint);
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @AfterAll
  static void afterAll() throws IOException {
    CLUSTER.deleteTables(TABLE_NAME_FAMILY_LIST_MAP.keySet().toArray(new TableName[0]));
    CLUSTER.close();
  }

  private static TestModel createNewTestModel(String urlFingerprint) {
    return TestModel.builder()
        .url("https://www.trthaber.com/")
        .domain("com.trthaber.www")
        .htmlContent(HTML_DUMMY2)
        .crawlTime(Instant.now())
        .urlFingerprint(urlFingerprint)
        .build();
  }

  @Test
  void testSaveOneRow() throws Exception {
    boolean put = cmdOperations.put(TABLE_NAME, "com.sozcu.www", CONTENTS_FAMILY_AND_COLUMN, HTML_CONTENT_VALUE1);
    assertTrue(put);
  }

  @Test
  void testSaveMultipleRows() throws Exception {
    //given:
    Map<String, byte[]> htmlData = new HashMap<>();
    htmlData.put("fingerPrint1", HTML_CONTENT_VALUE1);
    htmlData.put("fingerPrint2", HTML_CONTENT_VALUE2);
    //when:
    boolean put = cmdOperations.put(TABLE_NAME, CONTENTS_FAMILY_AND_COLUMN, htmlData);
    //then:
    assertTrue(put);
  }

  @Test
  void testSaveRowWithAllCells() throws Exception {
    //given:
    final TestModel expected = createNewTestModel("98asd55ddw");
    //when:
    boolean put = cmdOperations.put(TABLE_NAME, expected.toHRow());
    TestModel actual = queryOperations.get(TABLE_NAME, expected.getUrlFingerprint(), TestModelRowMapper.INSTANCE).get();
    //then:
    assertTrue(put);
    assertEquals(expected, actual);
  }

  @Test
  void testSaveBatchRowWithAllCells() throws Exception {
    //given:
    TestModel newTestModel1 = createNewTestModel("18bfa2898ade");
    TestModel newTestModel2 = createNewTestModel("15abs7845sa");
    Map<String, Map<FamilyAndColumn, byte[]>> rowsToSave = new HashMap<>();
    rowsToSave.putAll(newTestModel1.toHRow());
    rowsToSave.putAll(newTestModel2.toHRow());
    //when:
    boolean put = cmdOperations.put(TABLE_NAME, rowsToSave);
    List<TestModel> resultList = queryOperations
        .get(TABLE_NAME, Arrays.asList("18bfa2898ade", "15abs7845sa"), TestModelRowMapper.INSTANCE);
    //then:
    assertTrue(put);
    assertEquals(Arrays.asList(newTestModel1, newTestModel2), resultList);
  }

  @Test
  void testDeleteOneRow() throws Exception {
    //given:
    Map<String, byte[]> htmlData = new HashMap<>();
    htmlData.put("fingerPrint1", HTML_CONTENT_VALUE1);
    htmlData.put("fingerPrint2", HTML_CONTENT_VALUE2);
    boolean put = cmdOperations.put(TABLE_NAME, CONTENTS_FAMILY_AND_COLUMN, htmlData);
    assertTrue(put);
    //when:
    boolean delete = cmdOperations.delete(TABLE_NAME, "fingerPrint1", CONTENTS_FAMILY_AND_COLUMN);
    //then:
    assertTrue(delete);
  }

  @Test
  void testDeleteMultipleRow() throws Exception {
    //given:
    Map<String, byte[]> htmlData = new HashMap<>();
    htmlData.put("fingerPrint3", HTML_CONTENT_VALUE1);
    htmlData.put("fingerPrint4", HTML_CONTENT_VALUE2);
    boolean putHtml = cmdOperations.put(TABLE_NAME, CONTENTS_FAMILY_AND_COLUMN, htmlData);
    assertTrue(putHtml);

    Map<String, byte[]> domainNameData = new HashMap<>();
    htmlData.put("fingerPrint3", Bytes.toBytes("com.trthaber.www"));
    htmlData.put("fingerPrint4", Bytes.toBytes("com.cnn.www"));
    boolean putDomain = cmdOperations.put(TABLE_NAME, ANCHOR_FAMILY_AND_DOMAIN_NAME_COLUMN, domainNameData);
    assertTrue(putDomain);

    //when:
    boolean delete = cmdOperations.delete(TABLE_NAME,
        Arrays.asList(CONTENTS_FAMILY_AND_COLUMN, ANCHOR_FAMILY_AND_DOMAIN_NAME_COLUMN),
        Arrays.asList("fingerPrint1", "fingerPrint2"));

    //then:
    assertTrue(delete);
  }

  @Test
  void testFindByFamilyStringQuery() throws Exception {
    //when:
    List<TestModel> results = queryOperations.find(TABLE_NAME, "contents", TestModelRowMapper.INSTANCE);
    //then:
    results.forEach(testModel -> System.out.println(testModel.toString()));
    assertFalse(results.isEmpty());
  }

  @Test
  void testFindByColumnFamilyAndColumnPairQuery1() throws Exception {
    //when:
    List<TestModel> results = queryOperations.find(TABLE_NAME, ANCHOR_FAMILY_AND_URL_COLUMN, TestModelRowMapper.INSTANCE);
    results.forEach(testModel -> System.out.println(testModel.toString()));
    //then:
    assertFalse(results.isEmpty());
  }

  @Test
  void testFindByColumnFamilyAndColumnPairQuery2() throws Exception {
    //given:
    ResultsExtractor<List<TestModel>> resultExtractor = new RowMapperResultExtractor<>(TestModelRowMapper.INSTANCE);
    //when:
    List<TestModel> results = queryOperations.find(TABLE_NAME, ANCHOR_FAMILY_AND_CRAWL_TIME_COLUMN, resultExtractor);
    results.forEach(testModel -> System.out.println(testModel.toString()));
    //then:
    assertFalse(results.isEmpty());
  }

  @Test
  void testFindByColumnFamilyQuery() throws Exception {
    //given:
    ResultsExtractor<List<TestModel>> resultExtractor = new RowMapperResultExtractor<>(TestModelRowMapper.INSTANCE);
    //when:
    List<TestModel> results = queryOperations.find(TABLE_NAME, "anchor", resultExtractor);
    results.forEach(testModel -> System.out.println(testModel.toString()));
    //then:
    assertFalse(results.isEmpty());
  }


  @Test
  void testGetByRowKeysQuery() {
    //given:
    List<String> rowKeys = Arrays.asList("fingerPrint1", "fingerPrint2");
    //when:
    List<TestModel> results = queryOperations.get(TABLE_NAME, rowKeys, TestModelRowMapper.INSTANCE);
    results.forEach(testModel -> System.out.println(testModel.toString()));
    //then:
    assertFalse(results.isEmpty());
  }

  @Test
  void testRowKeyRangeQuery() {
    //given:
    RangeQueryParameters rangeQueryParameters = RangeQueryParameters.builder()
        .startRowKey("fingerPrint1")
        .startRowInclusive(true)
        .endRowKey("fingerPrint2")
        .endRowInclusive(true)
        .build();
    ResultsExtractor<List<TestModel>> resultExtractor = new RowMapperResultExtractor<>(TestModelRowMapper.INSTANCE);
    //when:
    List<TestModel> results = queryOperations.get(TABLE_NAME, rangeQueryParameters, resultExtractor);
    results.forEach(testModel -> System.out.println(testModel.toString()));
    //then:
    assertFalse(results.isEmpty());
  }

  @Test
  void testGetBySingleRowKeyQuery() {
    //when:
    TestModel result = queryOperations.get(TABLE_NAME, "fingerPrint1", TestModelRowMapper.INSTANCE).get();
    //then:
    System.out.println(result.toString());
    assertNotNull(result);
  }

  @Test
  void testGetByRowKeyAndFamilyPairQuery() {
    //when:
    TestModel result = queryOperations.get(TABLE_NAME, "fingerPrint1", CONTENTS_FAMILY_AND_COLUMN, TestModelRowMapper.INSTANCE).get();
    //then:
    System.out.println(result.toString());
    assertNotNull(result);
  }

  enum TestModelRowMapper implements RowMapper<TestModel> {
    INSTANCE;

    @SneakyThrows
    @Override
    public Optional<TestModel> mapRow(Result result, int rowNum) {
      return Optional.of(TestModel.builder()
          .domain(Bytes.toString(result.getValue(ANCHOR_FAMILY_AND_DOMAIN_NAME_COLUMN.getFamilyBytes(),
              ANCHOR_FAMILY_AND_DOMAIN_NAME_COLUMN.getColumnBytes())))
          .crawlTime(deserialize(result.getValue(ANCHOR_FAMILY_AND_CRAWL_TIME_COLUMN.getFamilyBytes(),
              ANCHOR_FAMILY_AND_CRAWL_TIME_COLUMN.getColumnBytes())))
          .htmlContent(Bytes.toString(result.getValue(CONTENTS_FAMILY_AND_COLUMN.getFamilyBytes(),
              CONTENTS_FAMILY_AND_COLUMN.getColumnBytes())))
          .url(Bytes.toString(result.getValue(ANCHOR_FAMILY_AND_URL_COLUMN.getFamilyBytes(),
              ANCHOR_FAMILY_AND_URL_COLUMN.getColumnBytes())))
          .urlFingerprint(Bytes.toString(result.getValue(ANCHOR_FAMILY_AND_URL_FINGERPRINT_COLUMN.getFamilyBytes(),
              ANCHOR_FAMILY_AND_URL_FINGERPRINT_COLUMN.getColumnBytes())))
          .build());
    }
  }

  @Data
  @Builder
  @AllArgsConstructor
  @ToString
  static class TestModel implements Serializable {

    String domain;
    Instant crawlTime;
    String htmlContent;
    String url;
    String urlFingerprint;

    @SneakyThrows
    Map<String, Map<FamilyAndColumn, byte[]>> toHRow() {
      Map<FamilyAndColumn, byte[]> columnMap = new HashMap<>(5);
      columnMap.put(ANCHOR_FAMILY_AND_CRAWL_TIME_COLUMN, serialize(crawlTime));
      columnMap.put(ANCHOR_FAMILY_AND_URL_FINGERPRINT_COLUMN, Bytes.toBytes(urlFingerprint));
      columnMap.put(ANCHOR_FAMILY_AND_DOMAIN_NAME_COLUMN, Bytes.toBytes(domain));
      columnMap.put(ANCHOR_FAMILY_AND_URL_COLUMN, Bytes.toBytes(url));
      columnMap.put(CONTENTS_FAMILY_AND_COLUMN, Bytes.toBytes(htmlContent));
      Map<String, Map<FamilyAndColumn, byte[]>> row = new HashMap<>(1);
      row.put(urlFingerprint, columnMap);
      return row;
    }
  }
}