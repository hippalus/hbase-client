package com.github.hippalus.hbase.common;

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public final class FamilyAndColumn extends Pair<String, String> {

  private FamilyAndColumn(String family, String columnQualifier) {
    super(family, columnQualifier);
  }

  public static FamilyAndColumn valueOf(@Nonnull String family, @Nullable String columnQualifier) {
    Objects.requireNonNull(family, "Family name  required");
    return new FamilyAndColumn(family, columnQualifier);
  }

  public byte[] getFamilyBytes() {
    return Bytes.toBytes(this.first);
  }

  public byte[] getColumnBytes() {
    return Bytes.toBytes(this.second);
  }

  public boolean columnIsNonNull() {
    return this.second != null;
  }
}
