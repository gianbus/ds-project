package it.polimi.ds.client;

import it.polimi.ds.Value;
import it.polimi.ds.rmi.ClusterInfo;
import it.polimi.ds.rmi.RemoteInfo;
import it.polimi.ds.rmi.Replica;
import it.polimi.ds.server.Node;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class TestClient {
    @Test
    public void TestGet() {
        ClusterInfo clusterInfo = new ClusterInfo(new RemoteInfo[]{
                new RemoteInfo("A", ""),
                new RemoteInfo("B", ""),
                new RemoteInfo("C", "")
        }, 2, 2);
        Connector mockConnector = buildMockConnector(clusterInfo);

        try {
            Middleware middleware = new LeaderlessMiddleware(mockConnector, clusterInfo.getRemoteInfos()[0]);

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
        ClusterInfo clusterInfo = new ClusterInfo(new RemoteInfo[]{
                new RemoteInfo("A", ""),
                new RemoteInfo("B", ""),
                new RemoteInfo("C", "")
        }, 2, 2);
        Connector mockConnector = buildMockConnector(clusterInfo);

        try {
            Middleware middleware1 = new LeaderlessMiddleware(mockConnector, clusterInfo.getRemoteInfos()[0]);
            Middleware middleware2 = new LeaderlessMiddleware(mockConnector, clusterInfo.getRemoteInfos()[1]);
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

    @Test
    public void TestRepair() {
        ClusterInfo clusterInfo = new ClusterInfo(new RemoteInfo[]{
                new RemoteInfo("A", ""),
                new RemoteInfo("B", ""),
                new RemoteInfo("C", "")
        }, 3, 2);
        Connector mockConnector = buildMockConnector(clusterInfo);

        AtomicReference<ExecutorService> lastPool = new AtomicReference<>();
        Function<Integer, ExecutorService> poolFactory = (n) -> {
            ExecutorService pool = Executors.newFixedThreadPool(n);
            lastPool.set(pool);
            return pool;
        };

        try {
            Middleware middleware = new LeaderlessMiddleware(mockConnector, clusterInfo.getRemoteInfos()[0], poolFactory);

            Assert.assertTrue(middleware.Put("key", "value"));
            // Get will read+repair and set lastPool
            middleware.Get("key");

            lastPool.get().shutdown();
            Assert.assertTrue("test timeout after 10 seconds",
                    lastPool.get().awaitTermination(10, TimeUnit.SECONDS));

            for (RemoteInfo remoteInfo : clusterInfo.getRemoteInfos()) {
                Replica replica = mockConnector.Connect(remoteInfo);
                Value value = replica.Read("key");
                Assert.assertNotNull(value);
                Assert.assertEquals("value", value.getValue());
            }
        } catch (Exception e) {
            Assert.fail("unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void TestQueue() {
        int nMiddlewares = 20;
        int nRounds = 100;
        int nReplicas = 10;
        int r = 6;
        int w = 6;
        RemoteInfo[] remoteInfos = new RemoteInfo[nReplicas];

        for (int i = 0; i < nReplicas; i++) {
            remoteInfos[i] = new RemoteInfo(String.valueOf(i), "");
        }

        ClusterInfo clusterInfo = new ClusterInfo(remoteInfos, r, w);
        Connector mockConnector = buildMockConnector(clusterInfo);

        try {
            Thread[] threads = new Thread[nMiddlewares];
            Middleware[] middlewares = new Middleware[nMiddlewares];

            for (int i = 0; i < nMiddlewares;i++) {
                middlewares[i] = new LeaderlessMiddleware(mockConnector, clusterInfo.getRemoteInfos()[i % clusterInfo.getRemoteInfos().length]);
            }

            for (int i = 0; i < nRounds; i++) {
                AtomicReference<Integer> success = new AtomicReference<>(0);

                for (int j = 0; j < nMiddlewares; j++) {
                    int finalJ = j;
                    threads[j] = new Thread(() -> {
                        try {
                            boolean outcome = middlewares[finalJ].Put("x", String.valueOf(finalJ));
                            success.accumulateAndGet(outcome ? 1 : 0, Integer::sum);
                        } catch (Exception e) {
                            Assert.fail("unexpected exception: " + e.getMessage());
                        }
                    });
                    threads[j].start();
                }

                for (int j = 0; j < nMiddlewares; j++) {
                    threads[j].join();
                }

                Assert.assertTrue(success.get() >= 1);
            }
        } catch (Exception e) {
            Assert.fail("unexpected exception: " + e.getMessage());
        }
    }

    private Connector buildMockConnector(ClusterInfo clusterInfo) {
        HashMap<RemoteInfo, Replica> replicaMap = new HashMap<>();
        for (RemoteInfo remoteInfo : clusterInfo.getRemoteInfos()) {
            replicaMap.put(remoteInfo, new Node(clusterInfo));
        }
        return new MockConnector(replicaMap);
    }
}
