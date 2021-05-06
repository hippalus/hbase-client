package com.github.hippalus.hbase.common;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.hbase.util.Bytes;


@Builder
@RequiredArgsConstructor
@Getter
public class RangeQueryParameters {

  private final String startRowKey;
  private final boolean startRowInclusive;
  private final String endRowKey;
  private final boolean endRowInclusive;

  public byte[] getStartRowKeyBytes() {
    return Bytes.toBytes(startRowKey);
  }


  public byte[] getEndRowKeyBytes() {
    return Bytes.toBytes(endRowKey);
  }

}
