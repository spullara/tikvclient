package com.sampullara.pingcap;

import com.google.protobuf.ByteString;
import kvrpcpb.Kvrpcpb;
import msgpb.Msgpb;
import pdpb.Pdpb;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

/**
 * Created by sam on 2/6/17.
 */
public class TiTx implements Tx {
  private ByteString primaryKey;
  private TiKV tiKV;
  private long timestamp;

  public TiTx(TiKV tiKV) {
    this.tiKV = tiKV;
  }

  private Map<ByteString, ByteString> cachedWrites = new HashMap<>();
  private Set<ByteString> cachedDeletes = new HashSet<>();

  @Override
  public void set(byte[] k, byte[] value) {
    ByteString key = ByteString.copyFrom(k);
    if (primaryKey == null) {
      primaryKey = key;
    }
    cachedWrites.put(key, ByteString.copyFrom(value));
    cachedDeletes.remove(key);
  }

  @Override
  public byte[] get(byte[] k) {
    ByteString key = ByteString.copyFrom(k);
    ByteString bytes = cachedWrites.get(key);
    if (bytes != null) {
      // we haven't written these yet
      return bytes.toByteArray();
    }
    TiKV.GetLocation keyLocation = tiKV.getKeyLocation(key);
    Kvrpcpb.Context context = keyLocation.getContext();
    RPC kvRPC = keyLocation.getKvRPC();

    Kvrpcpb.CmdGetRequest.Builder getReq = Kvrpcpb.CmdGetRequest.newBuilder();
    getReq.setKey(key);
    if (timestamp == 0) {
      timestamp = tiKV.getTimestamp();
    }

    getReq.setVersion(timestamp);
    Msgpb.Message.Builder getMessage = Msgpb.Message.newBuilder();
    getMessage.setMsgType(Msgpb.MessageType.KvReq);
    Kvrpcpb.Request.Builder kvReq = Kvrpcpb.Request.newBuilder()
            .setContext(context)
            .setCmdGetReq(getReq)
            .setType(Kvrpcpb.MessageType.CmdGet);
    getMessage.setKvReq(kvReq);
    Msgpb.Message getResponse = null;
    try {
      getResponse = kvRPC.send(getMessage.build());
      Kvrpcpb.CmdGetResponse cmdGetResp = getResponse.getKvResp().getCmdGetResp();
      Kvrpcpb.KeyError error = cmdGetResp.getError();
      if (error != null) {
        // Need to retry transactions that retryable / restart on abort
        throw new TiException("Error getting key: " + error);
      }
      return cmdGetResp.getValue().toByteArray();
    } catch (IOException e) {
      throw new TiException("Failed to get key", e);
    }
  }

  @Override
  public void clear(byte[] k) {
    ByteString key = ByteString.copyFrom(k);
    cachedDeletes.add(key);
    cachedWrites.remove(key);
  }

  public void commit() {
    TiKV.GetLocation keyLocation = tiKV.getKeyLocation(primaryKey);
    Kvrpcpb.Context context = keyLocation.getContext();
    RPC kvRPC = keyLocation.getKvRPC();

    {
      // Need to send prewrite requests to each KV involved in the transaction
      Kvrpcpb.CmdPrewriteRequest.Builder preWriteReq = Kvrpcpb.CmdPrewriteRequest.newBuilder();
      cachedWrites.entrySet().forEach(e -> {
        Kvrpcpb.Mutation.Builder builder = preWriteReq.addMutationsBuilder();
        builder.setKey(e.getKey());
        builder.setValue(e.getValue());
        builder.setOp(Kvrpcpb.Op.Put);
        preWriteReq.addMutations(builder.build());
      });
      cachedDeletes.forEach(e -> {
        Kvrpcpb.Mutation.Builder builder = preWriteReq.addMutationsBuilder();
        builder.setKey(e);
        builder.setOp(Kvrpcpb.Op.Del);
        preWriteReq.addMutations(builder.build());
      });
      preWriteReq.setPrimaryLock(primaryKey);
      preWriteReq.setStartVersion(timestamp);
      // I don't know what this should be
      preWriteReq.setLockTtl(5000);

      Msgpb.Message.Builder getMessage = Msgpb.Message.newBuilder();
      getMessage.setMsgType(Msgpb.MessageType.KvReq);
      Kvrpcpb.Request.Builder kvReq = Kvrpcpb.Request.newBuilder()
              .setCmdPrewriteReq(preWriteReq)
              .setType(Kvrpcpb.MessageType.CmdGet);
      getMessage.setKvReq(kvReq);
      Msgpb.Message getResponse = null;
      try {
        getResponse = kvRPC.send(getMessage.build());
        Kvrpcpb.CmdPrewriteResponse cmdPrewriteResponse = getResponse.getKvResp().getCmdPrewriteResp();
        List<Kvrpcpb.KeyError> error = cmdPrewriteResponse.getErrorsList();
        if (error != null && error.size() > 0) {
          // Need to retry transactions that retryable / restart on abort
          throw new TiException("Error prewriting transaction: " + error);
        }
        // prewritten
      } catch (IOException e) {
        throw new TiException("Failed to prewrite", e);
      }
    }

    {
      long commitVersion = tiKV.getTimestamp();
      Kvrpcpb.CmdCommitRequest.Builder commitReq = Kvrpcpb.CmdCommitRequest.newBuilder();
      commitReq.setStartVersion(timestamp);
      commitReq.setCommitVersion(commitVersion);
      commitReq.setBinlog(ByteString.copyFrom("Not sure what this is".getBytes()));
      Msgpb.Message.Builder getMessage = Msgpb.Message.newBuilder();
      getMessage.setMsgType(Msgpb.MessageType.KvReq);
      Kvrpcpb.Request.Builder kvReq = Kvrpcpb.Request.newBuilder()
              .setCmdCommitReq(commitReq)
              .setType(Kvrpcpb.MessageType.CmdCommit);
      getMessage.setKvReq(kvReq);
      Msgpb.Message getResponse = null;
      try {
        getResponse = kvRPC.send(getMessage.build());
        Kvrpcpb.CmdCommitResponse cmdPrewriteResponse = getResponse.getKvResp().getCmdCommitResp();
        Kvrpcpb.KeyError error = cmdPrewriteResponse.getError();
        if (error != null) {
          // Need to retry transactions that retryable / restart on abort
          throw new TiException("Error prewriting transaction: " + error);
        }
        // prewritten
      } catch (IOException e) {
        throw new TiException("Failed to commit", e);
      }
    }
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
