package com.sampullara.pingcap;

import java.util.stream.Stream;

/**
 * Created by sam on 2/6/17.
 */
public class TiTx implements Tx {
  private TiKV tiKV;

  public TiTx(TiKV tiKV) {
    this.tiKV = tiKV;
  }

  @Override
  public void set(byte[] key, byte[] value) {

  }

  @Override
  public byte[] get(byte[] key) {
    return new byte[0];
  }

  @Override
  public Stream<KV> scan(byte[] startKey, int limit) {
    return null;
  }

  @Override
  public Stream<byte[]> scanKeys(byte[] startKey, int limit) {
    return null;
  }
}
