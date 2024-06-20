package com.github.wenweihu86.raft.example.client;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.github.wenweihu86.raft.example.server.service.ExampleProto;
import com.github.wenweihu86.raft.example.server.service.ExampleService;


public class FWHTest {
    public static void main(String[] args) {
        String ipPorts = "list://127.0.0.1:13040,127.0.0.1:13041,127.0.0.1:13042";
        RpcClient rpcClient = new RpcClient(ipPorts);
        rpcClient.getRpcClientOptions().setReadTimeoutMillis(10000);
        rpcClient.getRpcClientOptions().setWriteTimeoutMillis(10000);
        ExampleService exampleService = BrpcProxy.getProxy(rpcClient, ExampleService.class);

        Map<String, String> map = new HashMap<>();

        for (int i = 0; i < 1000; i++) {
            String key = UUID.randomUUID().toString();
            String value = UUID.randomUUID().toString();
            map.put(key, value);

            System.out.println("Test "+ i + ": " + key + ", " + value);

            ExampleProto.SetRequest setRequest = ExampleProto.SetRequest.newBuilder().setKey(key).setValue(value)
                    .build();
            ExampleProto.SetResponse setResponse = exampleService.set(setRequest);
            System.out.print(setResponse);
        }

        for (String k: map.keySet()) {
            String v = map.get(k);
            ExampleProto.GetRequest getRequest = ExampleProto.GetRequest.newBuilder().setKey(k).build();
            ExampleProto.GetResponse getResponse = exampleService.get(getRequest);
            System.out.print(getResponse);
            String realV = getResponse.getValue();
            if (!realV.equals(v)) {
                System.out.println("Expected: " + v + ", " + "but: " + realV);
                throw new RuntimeException("Expected: " + v + ", " + "but: " + realV);
            }
        }
        rpcClient.stop();
    }
}
