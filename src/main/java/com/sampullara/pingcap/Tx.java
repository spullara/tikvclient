package com.sampullara.pingcap;

import java.util.stream.Stream;

/**
 * Created by sam on 2/6/17.
 */
public interface Tx {
  void set(byte[] key, byte[] value);
  byte[] get(byte[] key);
  void clear(byte[] key);
  Stream<KV> scan(byte[] startKey, int limit);
  Stream<byte[]> scanKeys(byte[] startKey, int limit);
}
