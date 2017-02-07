package com.sampullara.pingcap;

import com.google.protobuf.ByteString;
import kvrpcpb.Kvrpcpb;
import metapb.Metapb;
import msgpb.Msgpb;
import pdpb.Pdpb;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.sampullara.pingcap.RPC.RPCType.KV;

/**
 * Created by sam on 2/6/17.
 */
public class TiKV implements DB {

  private final RPC pdRPC;
  private final long clusterId;
  private List<String> pdList;

  public TiKV(String... pds) {
    // try and connect right away to ensure that we can
    pdRPC = getRpc(pds);
    // get the cluster id
    try {
      Pdpb.Response response = pdRPC.send(Pdpb.Request.newBuilder()
              .setCmdType(Pdpb.CommandType.GetPDMembers)
              .setGetPdMembers(Pdpb.GetPDMembersRequest.newBuilder()).build());
      pdList = response.getGetPdMembers().getMembersList().stream()
              .flatMap(pdm -> pdm.getPeerUrlsList().asByteStringList().stream()
                      // convert to a string
                      .map(ByteString::toStringUtf8)
                      // they are http:// urls
                      .map(s -> s.substring(7)))
              .collect(Collectors.toList());
      clusterId = response.getHeader().getClusterId();
    } catch (IOException e) {
      throw new TiException("Couldn't get cluster id from tikv", e);
    }
  }

  // Get a random connection to one of the PDs provided
  private RPC getRpc(String[] pds) {
    List<String> list = Arrays.asList(pds);
    Collections.shuffle(list);
    return list.stream()
            // format of the string is host:port
            .map(pd -> pd.split(":"))
            // try and connect
            .map(address -> {
              try {
                return new RPC(address[0], Integer.parseInt(address[1]), RPC.RPCType.PD);
              } catch (TiException tie) {
                return null;
              }
            })
            // if it connected then
            .filter(Objects::nonNull)
            // return it
            .findFirst()
            // if none connect then complain
            .orElseGet(() -> {
              throw new TiException("No PDs are reachable: " + Arrays.toString(pds));
            });
  }

  @Override
  public <T> T run(Function<Tx, T> f) {
    Tx tx = new TiTx(this);
    return f.apply(tx);
  }

  private static Pdpb.Request.Builder createRequest(Pdpb.CommandType cmdType, long clusterId) {
    return Pdpb.Request.newBuilder()
            .setCmdType(cmdType)
            .setHeader(Pdpb.RequestHeader.newBuilder()
                    .setClusterId(clusterId)
                    .setUuid(ByteString.copyFrom(UUID.randomUUID().toString().getBytes())));
  }

  @Override
  public void set(byte[] k, byte[] v) {
    // Almost everything in this method needs to be cached

    ByteString key = ByteString.copyFrom(k);
    ByteString value = ByteString.copyFrom(v);

    GetLocation getLocation = new GetLocation(key).invoke();

    Kvrpcpb.Context context = getLocation.getContext();
    RPC kvRPC = getLocation.getKvRPC();

    Kvrpcpb.CmdRawPutRequest.Builder putReq = Kvrpcpb.CmdRawPutRequest.newBuilder();
    putReq.setKey(key);
    putReq.setValue(value);
    Msgpb.Message.Builder putMessage = Msgpb.Message.newBuilder();
    putMessage.setMsgType(Msgpb.MessageType.KvReq);
    putMessage.setKvReq(Kvrpcpb.Request.newBuilder().setContext(context).setCmdRawPutReq(putReq).setType(Kvrpcpb.MessageType.CmdRawPut));
    Msgpb.Message putResponse;
    try {
      putResponse = kvRPC.send(putMessage.build());
      String error = putResponse.getKvResp().getCmdRawPutResp().getError();
      if (error != null && !error.equals("")) {
        throw new TiException("Failed to write: " + error);
      }
    } catch (IOException e) {
      throw new TiException("Failed to set", e);
    }
  }

  @Override
  public byte[] get(byte[] k) {
    ByteString key = ByteString.copyFrom(k);
    GetLocation getLocation = new GetLocation(key).invoke();
    Kvrpcpb.Context context = getLocation.getContext();
    RPC kvRPC = getLocation.getKvRPC();

    Kvrpcpb.CmdRawGetRequest.Builder getReq = Kvrpcpb.CmdRawGetRequest.newBuilder();
    getReq.setKey(key);
    Msgpb.Message.Builder getMessage = Msgpb.Message.newBuilder();
    getMessage.setMsgType(Msgpb.MessageType.KvReq);
    Kvrpcpb.Request.Builder kvReq = Kvrpcpb.Request.newBuilder()
            .setContext(context)
            .setCmdRawGetReq(getReq)
            .setType(Kvrpcpb.MessageType.CmdRawGet);
    getMessage.setKvReq(kvReq);
    Msgpb.Message getResponse = null;
    try {
      getResponse = kvRPC.send(getMessage.build());
      Kvrpcpb.CmdRawGetResponse cmdRawGetResp = getResponse.getKvResp().getCmdRawGetResp();
      String error = cmdRawGetResp.getError();
      if (error != null && !error.equals((""))) {
        throw new TiException("Error getting key: " + error);
      }
      return cmdRawGetResp.getValue().toByteArray();
    } catch (IOException e) {
      throw new TiException("Failed to get key", e);
    }
  }

  private class GetLocation {
    private ByteString key;
    private RPC kvRPC;
    private Kvrpcpb.Context context;

    public GetLocation(ByteString key) {
      this.key = key;
    }

    public RPC getKvRPC() {
      return kvRPC;
    }

    public Kvrpcpb.Context getContext() {
      return context;
    }

    public GetLocation invoke() {
      // Almost everything in this method needs to be cached somehow

      Pdpb.Response getRegionForKey;
      try {
        getRegionForKey = pdRPC.send(createRequest(Pdpb.CommandType.GetRegion, clusterId)
                .setGetRegion(Pdpb.GetRegionRequest.newBuilder()
                        .setRegionKey(key))
                .build());
      } catch (IOException e) {
        throw new TiException("Failed to get region for key", e);
      }
      Pdpb.GetRegionResponse getRegion = getRegionForKey.getGetRegion();
      long storeId = getRegion.getLeader().getStoreId();

      Kvrpcpb.Context.Builder contextBuilder = Kvrpcpb.Context.newBuilder();
      Optional<Metapb.Peer> first = getRegion.getRegion().getPeersList().stream().filter(p -> p.getId() == getRegion.getLeader().getId()).findFirst();
      first.ifPresent(contextBuilder::setPeer);
      contextBuilder.setRegionId(getRegion.getRegion().getId());
      contextBuilder.setRegionEpoch(getRegion.getRegion().getRegionEpoch());

      Pdpb.Response getStore;
      try {
        getStore = pdRPC.send(createRequest(Pdpb.CommandType.GetStore, clusterId)
                .setGetStore(Pdpb.GetStoreRequest.newBuilder()
                        .setStoreId(storeId))
                .build());
      } catch (IOException e) {
        throw new TiException("Failed to getStore for region", e);
      }
      String address = getStore.getGetStore().getStore().getAddress();
      String[] split = address.split(":");
      String host = split[0];
      int port = Integer.valueOf(split[1]);

      kvRPC = new RPC(host, port, KV);
      context = contextBuilder.build();
      return this;
    }
  }
}
