package com.github.wenweihu86.raft.example.server.service.impl;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.instance.Endpoint;
import com.github.wenweihu86.raft.Peer;
import com.github.wenweihu86.raft.example.server.ExampleStateMachine;
import com.github.wenweihu86.raft.example.server.service.ExampleProto;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.proto.RaftProto;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.googlecode.protobuf.format.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by wenweihu86 on 2017/5/9.
 */
public class ExampleServiceImpl implements ExampleService {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleServiceImpl.class);
    private static JsonFormat jsonFormat = new JsonFormat();

    private RaftNode raftNode;
    private ExampleStateMachine stateMachine;
    private int leaderId = -1;
    private RpcClient leaderRpcClient = null;
    private Lock leaderLock = new ReentrantLock();

    ExampleService exampleService = null;

    public ExampleServiceImpl(RaftNode raftNode, ExampleStateMachine stateMachine) {
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
    }

    private void onLeaderChangeEvent() {
        if (raftNode.getLeaderId() != -1
                && raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()
                && leaderId != raftNode.getLeaderId()) {
            leaderLock.lock();
            if (leaderId != -1 && leaderRpcClient != null) {
                leaderRpcClient.stop();
                leaderRpcClient = null;
                leaderId = -1;
            }
            leaderId = raftNode.getLeaderId();
            Peer peer = raftNode.getPeerMap().get(leaderId);
            Endpoint endpoint = new Endpoint(peer.getServer().getEndpoint().getHost(),
                    peer.getServer().getEndpoint().getPort());
            RpcClientOptions rpcClientOptions = new RpcClientOptions();
            rpcClientOptions.setGlobalThreadPoolSharing(true);
            leaderRpcClient = new RpcClient(endpoint, rpcClientOptions);
            leaderLock.unlock();
        }
    }

    @Override
    public synchronized ExampleProto.SetResponse set(ExampleProto.SetRequest request) {
        ExampleProto.SetResponse.Builder responseBuilder = ExampleProto.SetResponse.newBuilder();
        LOG.info("FWH: begin handle " + request.getKey() + "," + request.getValue());
        // 如果自己不是leader，将写请求转发给leader
        if (raftNode.getLeaderId() <= 0) {
            LOG.info("FWH: raftNode.getLeaderId() <= 0");
            responseBuilder.setSuccess(false);
        } else if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
            LOG.info("FWH:raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()");

            RpcClient oldLeaderRpcClient = leaderRpcClient;

            onLeaderChangeEvent();
            // raftNode.getLock().lock();
            System.out.println(leaderRpcClient.toString());

            Class c = leaderRpcClient.getServiceInterface();
            if (c == null) {
                System.out.println("leaderRpcClient.getServiceInterface() is null now.");
            } else {
                System.out.println("leaderRpcClient.getServiceInterface() is: " + c.toString() + ", getCanonicalName() is: " + c.getCanonicalName());
            }    

            
            // ExampleService exampleService = BrpcProxy.getProxy(leaderRpcClient, ExampleService.class);
            // ExampleProto.SetResponse responseFromLeader = exampleService.set(request);
            // responseBuilder.mergeFrom(responseFromLeader);
            if (leaderRpcClient != null && !leaderRpcClient.equals(oldLeaderRpcClient)) {
                exampleService = BrpcProxy.getProxy(leaderRpcClient, ExampleService.class);
            }
            if (leaderRpcClient != null) {
                ExampleProto.SetResponse responseFromLeader = exampleService.set(request);
                responseBuilder.mergeFrom(responseFromLeader);
            } 
            // raftNode.getLock().unlock();
            
        } else {
            LOG.info("FWH: The last branch");
            // 数据同步写入raft集群
            byte[] data = request.toByteArray();
            boolean success = raftNode.replicate(data, RaftProto.EntryType.ENTRY_TYPE_DATA);
            responseBuilder.setSuccess(success);
            
        }

        ExampleProto.SetResponse response = responseBuilder.build();
        LOG.info("set request, request={}, response={}", jsonFormat.printToString(request),
                jsonFormat.printToString(response));
        return response;
    }

    @Override
    public ExampleProto.GetResponse get(ExampleProto.GetRequest request) {
        ExampleProto.GetResponse response = stateMachine.get(request);
        LOG.info("get request, request={}, response={}", jsonFormat.printToString(request),
                jsonFormat.printToString(response));
        return response;
    }

}
