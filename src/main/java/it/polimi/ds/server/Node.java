package it.polimi.ds.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.polimi.ds.Transaction;
import it.polimi.ds.Value;
import it.polimi.ds.rmi.ClusterInfo;
import it.polimi.ds.rmi.Replica;
import it.polimi.ds.rmi.VoteMessage;

import java.io.File;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static it.polimi.ds.rmi.VoteMessage.MessageType.ABORT;
import static it.polimi.ds.rmi.VoteMessage.MessageType.COMMIT;

public class Node implements Replica {
    private static class QueueElement {
        private static int maxTimeout;

        private final String transactionID;
        private final Value value;
        private CountDownLatch latch;

        private QueueElement(String transactionID, Value value) {
            this.transactionID = transactionID;
            this.value = value;
            this.latch = null;
        }

        private void start() {
            if (this.latch != null) {
                throw new RuntimeException("latch already initialized");
            }
            this.latch = new CountDownLatch(1);
        }

        private void stop() {
            if (this.latch == null) {
                throw new RuntimeException("latch was not initialized");
            }
            this.latch.countDown();
        }

        private boolean waitReady() throws InterruptedException {
            if (this.latch == null) {
                return true;
            }
            return this.latch.await(maxTimeout, TimeUnit.SECONDS);
        }
    }

    private static final int defaultQueueMaxSize = 5;
    private static final int defaultMaxQueueTimeoutSeconds = 3;

    private final ClusterInfo clusterInfo;
    private final int queueMaxSize;
    private final ConcurrentHashMap<String, Value> data = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Transaction> transactionsById = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ArrayList<QueueElement>> queueByKey = new ConcurrentHashMap<>();

    public Node(ClusterInfo clusterInfo) {
        this(clusterInfo, defaultQueueMaxSize, defaultMaxQueueTimeoutSeconds);
    }

    public Node(ClusterInfo clusterInfo, int queueMaxSize, int maxQueueTimeoutSeconds) {
        this.clusterInfo = clusterInfo;
        this.queueMaxSize = queueMaxSize;
        QueueElement.maxTimeout = maxQueueTimeoutSeconds;
    }

    private QueueElement recordTransaction(Transaction t) {
        this.transactionsById.put(t.getTransactionID(), t);
        synchronized (this.queueByKey) {
            if (!this.queueByKey.containsKey(t.getKey())) {
                this.queueByKey.put(t.getKey(), new ArrayList<>());
            }
            ArrayList<QueueElement> queue = this.queueByKey.get(t.getKey());
            if (queue.size() >= this.queueMaxSize) {
                // abort if queue for this key is full
                // TODO : maybe consider global queue instead of a queue for each key
                return null;
            }

            Value lastValue = this.data.get(t.getKey());
            if (!queue.isEmpty()) {
                lastValue = queue.get(queue.size() - 1).value;
            }
            if (lastValue != null && !t.getValue().getVersioning().greaterThan(lastValue.getVersioning())) {
                // abort if there are newer versions (including other requests in queue)
                return null;
            }

            QueueElement elem = new QueueElement(t.getTransactionID(), t.getValue());

            if (!queue.isEmpty()) {
                // there are elements in front of the queue: start the latch
                elem.start();
            }
            queue.add(elem);

            return elem;
        }
    }

    private void removeTransaction(String transactionID) {
        Transaction t = this.transactionsById.get(transactionID);
        if (t != null) {
            this.transactionsById.remove(transactionID);
            synchronized (this.queueByKey) {
                ArrayList<QueueElement> queue = this.queueByKey.get(t.getKey());
                if (queue.isEmpty()) return;
                for (int i = 0; i < queue.size(); i++) {
                    if (queue.get(i).transactionID.equals(transactionID)) {
                        if (i == 0 && queue.size() > 1) {
                            QueueElement next = queue.get(1);
                            // next element can be processed
                            next.stop();
                        }
                        queue.remove(i);
                        return;
                    }
                }
            }
        }
    }

    @Override
    public ClusterInfo GetClusterInfo() throws RemoteException {
        return this.clusterInfo;
    }

    @Override
    public Value Read(String key) {
        if (key == null) {
            throw new IllegalArgumentException();
        }
        return data.get(key);
    }

    @Override
    public void Repair(String key, Value value) throws RemoteException {
        if (key == null || value == null) {
            throw new IllegalArgumentException();
        }
        synchronized (data) {
            Value currentValue = data.get(key);
            if (currentValue == null || value.getVersioning().greaterThan(currentValue.getVersioning())) {
                data.put(key, value);
            }
        }
    }

    @Override
    public VoteMessage Prepare(String key, Value value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException();
        }

        String transactionID = UUID.randomUUID().toString();
        synchronized (this.transactionsById) {
            while (this.transactionsById.containsKey(transactionID)) {
                transactionID = UUID.randomUUID().toString();
            }
        }
        Transaction t = new Transaction(transactionID, key, value);
        QueueElement elem = recordTransaction(t);
        if (elem == null) {
            return new VoteMessage(ABORT, transactionID);
        }

        try {
            if (!elem.waitReady()) {
                return new VoteMessage(ABORT, transactionID);
            }
            return new VoteMessage(COMMIT, transactionID);
        } catch (InterruptedException e) {
            return new VoteMessage(ABORT, transactionID);
        }
    }

    @Override
    public void Commit(String transactionID) {
        Transaction t = this.transactionsById.get(transactionID);
        if (t != null) {
            synchronized (data) {
                Value currentValue = data.get(t.getKey());
                if (currentValue == null || t.getValue().getVersioning().greaterThan(currentValue.getVersioning())) {
                    data.put(t.getKey(), t.getValue());
                }
            }
            removeTransaction(transactionID);
        }
    }

    @Override
    public void Abort(String transactionID) {
        removeTransaction(transactionID);
    }

    public static void main(String[] args) {
        Node node = null;
        try {
            ObjectMapper mapper = new ObjectMapper();

            String configurationPath = (args.length < 1) ? null : args[0];
            if (configurationPath == null)
                throw new IllegalArgumentException("You need to specify a configuration path!");

            String registryName = (args.length < 2) ? null : args[1];
            if (registryName == null) throw new IllegalArgumentException("You need to specify a registry name!");

            System.setProperty("java.rmi.server.hostname", "192.168.1.18");

            ClusterInfo clusterInfo = mapper.readValue(new File(configurationPath), ClusterInfo.class);
            if (!clusterInfo.getRegistryNames().contains(registryName))
                throw new IllegalArgumentException("The registry name specified is incorrect");

            node = new Node(clusterInfo);
            Replica stub = (Replica) UnicastRemoteObject.exportObject(node, 0);

            // Binding the stub
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind(registryName, stub);

            System.err.println("Node ready");
        } catch (Exception e) {
            System.err.println("Node Exception: " + e);
            e.printStackTrace();
        }

        Node finalNode = node;
        Thread printingHook = new Thread(() -> {
            try {
                UnicastRemoteObject.unexportObject(finalNode, true);
                System.out.println("Node stopped");
            } catch (NoSuchObjectException e) {
                e.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(printingHook);
    }
}