package com.sampullara.pingcap;

import com.google.protobuf.ByteString;
import kvrpcpb.Kvrpcpb;
import metapb.Metapb;
import msgpb.Msgpb;
import pdpb.Pdpb;

import java.io.*;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

/**
 * Hello world!
 */
public class App {

  private static LongAdder messageId = new LongAdder();

  static {
    messageId.add(new SecureRandom().nextLong());
  }

  private static ThreadLocal<ByteArrayOutputStream> cachedBaos = new ThreadLocal<ByteArrayOutputStream>() {
    @Override
    protected ByteArrayOutputStream initialValue() {
      return new ByteArrayOutputStream();
    }

    @Override
    public ByteArrayOutputStream get() {
      ByteArrayOutputStream baos = super.get();
      baos.reset();
      return baos;
    }
  };

  public static class RPCConnection {
    private final OutputStream os;
    private final InputStream is;

    public RPCConnection(String host, int port, String type) throws IOException {
      Socket socket = new Socket(host, port);
      os = new BufferedOutputStream(socket.getOutputStream());
      is = new BufferedInputStream(socket.getInputStream());

      switch (type) {
        case "pd":
          os.write(("GET /" + type + "/rpc HTTP/1.0\r\n\r\n").getBytes());
          os.flush();
          break;
        default:
      }
    }

    public Pdpb.Response send(Pdpb.Request request) throws IOException {
      return send(Msgpb.Message.newBuilder()
              .setMsgType(Msgpb.MessageType.PdReq)
              .setPdReq(request).build())
              .getPdResp();
    }

    public Msgpb.Message send(Msgpb.Message message) throws IOException {
      // Write the message
      ByteArrayOutputStream baos = cachedBaos.get();
      message.writeTo(baos);
      writeHeader(os, baos.size());
      baos.writeTo(os);
      os.flush();

      // Get the response — later this needs to be async
      byte[] responseBytes = readHeader(is);
      return Msgpb.Message.parseFrom(responseBytes);
    }

    private static void writeHeader(OutputStream os, int length) throws IOException {
      DataOutputStream dos = new DataOutputStream(os);
      // Magic value
      dos.write(new byte[]{(byte) 0xda, (byte) 0xf4});
      // Version
      dos.writeShort(1);
      // Message length
      dos.writeInt(length);
      // Message Id
      dos.writeLong(messageId.longValue());
      // Go to the next message id
      messageId.increment();
    }

    private static byte[] readHeader(InputStream is) throws IOException {
      DataInputStream dis = new DataInputStream(is);
      int magic = dis.readShort();
      int version = dis.readShort();
      int length = dis.readInt();
      long id = dis.readLong();
      byte[] bytes = new byte[length];
      dis.readFully(bytes);
      return bytes;
    }

  }

  private static Pdpb.Request.Builder createRequest(Pdpb.CommandType cmdType, long clusterId) {
    return Pdpb.Request.newBuilder()
            .setCmdType(cmdType)
            .setHeader(Pdpb.RequestHeader.newBuilder()
                    .setClusterId(clusterId)
                    .setUuid(ByteString.copyFrom(UUID.randomUUID().toString().getBytes())));
  }


  public static void main(String[] args) throws IOException {
    RPCConnection pdRPC = new RPCConnection("127.0.0.1", 2379, "pd");

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

      RPCConnection kvRPC = new RPCConnection(host, port, "kv");
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
