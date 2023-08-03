package it.polimi.ds.client;

import it.polimi.ds.rmi.RemoteInfo;
import it.polimi.ds.rmi.Replica;
import it.polimi.ds.server.Node;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class TestClient {
    @Test
    public void TestGet() {
        RemoteInfo[] remoteInfos = {
            new RemoteInfo("A", ""),
            new RemoteInfo("B", ""),
            new RemoteInfo("C", "")
        };
        Connector mockConnector = buildMockConnector(remoteInfos);

        try {
            Middleware middleware = new LeaderlessMiddleware(mockConnector, remoteInfos, 2, 2);

            Assert.assertNull(middleware.Get("test"));

            middleware.Put("test", "hello world");

            String v = middleware.Get("test");
            Assert.assertNotNull(v);
            Assert.assertEquals("hello world", v);
        } catch (Exception e) {
            Assert.fail("unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void TestMultipleClients() {
        RemoteInfo[] remoteInfos = {
            new RemoteInfo("A", ""),
            new RemoteInfo("B", ""),
            new RemoteInfo("C", "")
        };
        Connector mockConnector = buildMockConnector(remoteInfos);

        try {
            Middleware middleware1 = new LeaderlessMiddleware(mockConnector, remoteInfos, 2, 2);
            Middleware middleware2 = new LeaderlessMiddleware(mockConnector, remoteInfos, 2, 2);
            Thread thread1 = new Thread(() -> {
                try {
                    if (middleware1.Put("x", "a")) {
                        System.out.println("MW1: write(x, a) success");
                    } else {
                        System.out.println("MW1: write(x, a) failed");
                    }
                    if (middleware1.Put("y", "a")) {
                        System.out.println("MW1: write(y, a) success");
                    } else {
                        System.out.println("MW1: write(y, a) failed");
                    }
                } catch (Exception e) {
                    Assert.fail("unexpected exception: " + e.getMessage());
                }
            });
            Thread thread2 = new Thread(() -> {
                try {
                    if (middleware2.Put("x", "b")) {
                        System.out.println("MW2: write(x, b) success");
                    } else {
                        System.out.println("MW2: write(x, b) failed");
                    }
                    if (middleware2.Put("y", "b")) {
                        System.out.println("MW2: write(y, b) success");
                    } else {
                        System.out.println("MW2: write(y, b) failed");
                    }
                } catch (Exception e) {
                    Assert.fail("unexpected exception: " + e.getMessage());
                }
            });
            thread1.start();
            thread2.start();

            thread1.join();
            thread2.join();

            Assert.assertEquals(middleware1.Get("x"), middleware2.Get("x"));
            Assert.assertEquals(middleware1.Get("y"), middleware2.Get("y"));
        } catch (Exception e) {
            Assert.fail("unexpected exception: " + e.getMessage());
        }
    }

    private Connector buildMockConnector(RemoteInfo[] remoteInfos) {
        HashMap<RemoteInfo,Replica> replicaMap = new HashMap<>();
        for (RemoteInfo remoteInfo: remoteInfos) {
            replicaMap.put(remoteInfo, new Node());
        }
        return new MockConnector(replicaMap);
    }
}
