package com.github.hippalus.hbase.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public final class SerializationUtils {

  private SerializationUtils() {
    throw new AssertionError();
  }

  public static byte[] serialize(final Serializable obj) throws IOException {
    if (obj == null) {
      return new byte[0];
    }
    try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
      try (ObjectOutputStream o = new ObjectOutputStream(b)) {
        o.writeObject(obj);
      }
      return b.toByteArray();
    }
  }

  public static <T> T deserialize(final byte[] bytes) throws IOException, ClassNotFoundException {
    if (bytes == null) {
      return null;
    }
    try (ByteArrayInputStream b = new ByteArrayInputStream(bytes)) {
      try (ObjectInputStream o = new ObjectInputStream(b)) {
        @SuppressWarnings("unchecked") // may fail with CCE if serialised form is incorrect
        final T obj = (T) o.readObject();
        return obj;
      }
    }
  }
}