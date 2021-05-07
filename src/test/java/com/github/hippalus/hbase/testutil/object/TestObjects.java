package com.github.hippalus.hbase.testutil.object;

import com.github.hippalus.hbase.common.FamilyAndColumn;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public final class TestObjects {

  public static final FamilyAndColumn CONTENTS_FAMILY_AND_COLUMN = FamilyAndColumn.valueOf("contents", "html");
  public static final FamilyAndColumn ANCHOR_FAMILY_AND_DOMAIN_NAME_COLUMN = FamilyAndColumn.valueOf("anchor", "domainName");
  public static final FamilyAndColumn ANCHOR_FAMILY_AND_URL_COLUMN = FamilyAndColumn.valueOf("anchor", "url");
  public static final FamilyAndColumn ANCHOR_FAMILY_AND_URL_FINGERPRINT_COLUMN = FamilyAndColumn.valueOf("anchor", "urlFingerprint");
  public static final FamilyAndColumn ANCHOR_FAMILY_AND_CRAWL_TIME_COLUMN = FamilyAndColumn.valueOf("anchor", "time");
  public static final TableName TABLE_NAME = TableName.valueOf("table");
  public static final Map<TableName, List<String>> TABLE_NAME_FAMILY_LIST_MAP = Collections
      .singletonMap(TableName.valueOf("table"), Arrays.asList("contents", "anchor"));
  public static final String HTML_DUMMY1 = "<form action=\"#\" method=\"post\">\n"
      + "  <fieldset>\n"
      + "    <label for=\"name\">Name:</label>\n"
      + "    <input type=\"text\" id=\"name\" placeholder=\"Enter your \n"
      + "full name\" />\n"
      + "\n"
      + "    <label for=\"email\">Email:</label>\n"
      + "    <input type=\"email\" id=\"email\" placeholder=\"Enter \n"
      + "your email address\" />\n"
      + "\n"
      + "    <label for=\"message\">Message:</label>\n"
      + "    <textarea id=\"message\" placeholder=\"What's on your \n"
      + "mind?\"></textarea>\n"
      + "\n"
      + "    <input type=\"submit\" value=\"Send message\" />\n"
      + "\n"
      + "  </fieldset>\n"
      + "</form>";

  public static final String HTML_DUMMY2 = "<table class=\"data\">\n"
      + "  <tr>\n"
      + "    <th>Entry Header 1</th>\n"
      + "    <th>Entry Header 2</th>\n"
      + "    <th>Entry Header 3</th>\n"
      + "    <th>Entry Header 4</th>\n"
      + "  </tr>\n"
      + "  <tr>\n"
      + "    <td>Entry First Line 1</td>\n"
      + "    <td>Entry First Line 2</td>\n"
      + "    <td>Entry First Line 3</td>\n"
      + "    <td>Entry First Line 4</td>\n"
      + "  </tr>\n"
      + "  <tr>\n"
      + "    <td>Entry Line 1</td>\n"
      + "    <td>Entry Line 2</td>\n"
      + "    <td>Entry Line 3</td>\n"
      + "    <td>Entry Line 4</td>\n"
      + "  </tr>\n"
      + "  <tr>\n"
      + "    <td>Entry Last Line 1</td>\n"
      + "    <td>Entry Last Line 2</td>\n"
      + "    <td>Entry Last Line 3</td>\n"
      + "    <td>Entry Last Line 4</td>\n"
      + "  </tr>\n"
      + "</table>";
  public static final byte[] HTML_CONTENT_VALUE1 = Bytes.toBytes(HTML_DUMMY1);
  public static final byte[] HTML_CONTENT_VALUE2 = Bytes.toBytes(HTML_DUMMY2);

  private TestObjects() {
  }

}
