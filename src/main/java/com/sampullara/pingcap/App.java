package com.sampullara.pingcap;

import com.google.protobuf.ByteString;
import kvrpcpb.Kvrpcpb;
import metapb.Metapb;
import msgpb.Msgpb;
import pdpb.Pdpb;

import java.io.*;
import java.util.Optional;
import java.util.UUID;

import static com.sampullara.pingcap.RPC.RPCType.KV;
import static com.sampullara.pingcap.RPC.RPCType.PD;

/**
 * Hello world!
 */
public class App {

  private static Pdpb.Request.Builder createRequest(Pdpb.CommandType cmdType, long clusterId) {
    return Pdpb.Request.newBuilder()
            .setCmdType(cmdType)
            .setHeader(Pdpb.RequestHeader.newBuilder()
                    .setClusterId(clusterId)
                    .setUuid(ByteString.copyFrom(UUID.randomUUID().toString().getBytes())));
  }


  public static void main(String[] args) throws IOException {
    RPC pdRPC = new RPC("127.0.0.1", 2379, PD);

    ByteString key = ByteString.copyFrom("foo", "UTF-8");
    ByteString value = ByteString.copyFrom("bar", "UTF-8");

    Pdpb.Response response = pdRPC.send(Pdpb.Request.newBuilder()
            .setCmdType(Pdpb.CommandType.GetPDMembers)
            .setGetPdMembers(Pdpb.GetPDMembersRequest.newBuilder()).build());
    long clusterId = response.getHeader().getClusterId();

    {
      Pdpb.Response getRegionForKey = pdRPC.send(createRequest(Pdpb.CommandType.GetRegion, clusterId)
              .setGetRegion(Pdpb.GetRegionRequest.newBuilder()
                      .setRegionKey(key))
              .build());
      Pdpb.GetRegionResponse getRegion = getRegionForKey.getGetRegion();
      System.out.println(getRegion);
      long storeId = getRegion.getLeader().getStoreId();

      Pdpb.Response getStore = pdRPC.send(createRequest(Pdpb.CommandType.GetStore, clusterId)
              .setGetStore(Pdpb.GetStoreRequest.newBuilder()
                      .setStoreId(storeId))
              .build());

      System.out.println(getStore);
      String address = getStore.getGetStore().getStore().getAddress();
      String[] split = address.split(":");
      String host = split[0];
      int port = Integer.valueOf(split[1]);

      System.out.println(address);

      RPC kvRPC = new RPC(host, port, KV);
      {
        Kvrpcpb.CmdRawPutRequest.Builder putReq = Kvrpcpb.CmdRawPutRequest.newBuilder();
        putReq.setKey(key);
        putReq.setValue(value);
        Msgpb.Message.Builder putMessage = Msgpb.Message.newBuilder();
        putMessage.setMsgType(Msgpb.MessageType.KvReq);
        Kvrpcpb.Context.Builder context = Kvrpcpb.Context.newBuilder();
        Optional<Metapb.Peer> first = getRegion.getRegion().getPeersList().stream().filter(p -> p.getId() == getRegion.getLeader().getId()).findFirst();
        first.ifPresent(context::setPeer);
        context.setRegionId(getRegion.getRegion().getId());
        context.setRegionEpoch(getRegion.getRegion().getRegionEpoch());
        putMessage.setKvReq(Kvrpcpb.Request.newBuilder().setContext(context).setCmdRawPutReq(putReq).setType(Kvrpcpb.MessageType.CmdRawPut));
        System.out.println(putMessage);
        Msgpb.Message putResponse = kvRPC.send(putMessage.build());
        System.out.println(putResponse);
      }
      {
        Kvrpcpb.CmdRawGetRequest.Builder getReq = Kvrpcpb.CmdRawGetRequest.newBuilder();
        getReq.setKey(key);
        Msgpb.Message.Builder getMessage = Msgpb.Message.newBuilder();
        getMessage.setMsgType(Msgpb.MessageType.KvReq);
        Kvrpcpb.Context.Builder context = Kvrpcpb.Context.newBuilder();
        Optional<Metapb.Peer> first = getRegion.getRegion().getPeersList().stream().filter(p -> p.getId() == getRegion.getLeader().getId()).findFirst();
        first.ifPresent(context::setPeer);
        context.setRegionId(getRegion.getRegion().getId());
        context.setRegionEpoch(getRegion.getRegion().getRegionEpoch());
        Kvrpcpb.Request.Builder kvReq = Kvrpcpb.Request.newBuilder()
                .setContext(context)
                .setCmdRawGetReq(getReq)
                .setType(Kvrpcpb.MessageType.CmdRawGet);
        getMessage.setKvReq(kvReq);
        System.out.println(getMessage);
        Msgpb.Message getResponse = kvRPC.send(getMessage.build());
        System.out.println(getResponse);
      }
    }
  }

}
