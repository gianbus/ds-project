package it.polimi.ds.client;

import it.polimi.ds.Value;
import it.polimi.ds.rmi.Replica;
import it.polimi.ds.rmi.VoteMessage;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static it.polimi.ds.rmi.VoteMessage.MessageType.ABORT;
import static it.polimi.ds.rmi.VoteMessage.MessageType.COMMIT;

public class LeaderlessMiddleware implements Middleware {
    private final Replica[] stubs;
    private final int r;
    private final int w;

    public LeaderlessMiddleware(Connector connector, RemoteInfo[] remoteInfos, int r, int w) throws Exception {
        int n = remoteInfos.length;
        if (2 * w <= n) {
            throw new RuntimeException("invalid w quorum");
        }
        if (r + w <= n) {
            throw new RuntimeException("invalid r+w quorum");
        }
        this.stubs = new Replica[n];
        for (int i = 0; i < n; i++) {
            this.stubs[i] = connector.Connect(remoteInfos[i]);
        }
        this.r = r;
        this.w = w;
    }

    @Override
    public String Get(String k) throws Exception {
        Value value = this.read(k);
        if (value == null) {
            return null;
        }
        return value.getValue();
    }

    @Override
    public boolean Put(String k, String v) throws Exception {
        Value currentValue = this.read(k);
        long versionNumber = currentValue == null ? 1 : currentValue.getVersioning().getVersion() + 1;
        Value.Versioning versioning = new Value.Versioning(versionNumber);
        final Value value = new Value(versioning, v);

        ExecutorService pool = Executors.newFixedThreadPool(this.w);
        Future<VoteMessage>[] futures = new Future[this.w];

        int[] shuffleArray = generateShuffleArray(this.stubs.length, this.w);
        // -> send prepare to selected replicas
        for (int i = 0; i < this.w; i++) {
            futures[i] = pool.submit(this.prepare(this.stubs[shuffleArray[i]], k, value));
        }

        // <- wait for votes
        VoteMessage.MessageType result = COMMIT;
        String[] transactionIDs = new String[this.w];
        for (int i = 0; i < this.w; i++) {
            VoteMessage vote = futures[i].get();
            if (vote.getType() == ABORT) {
                result = ABORT;
            }
            transactionIDs[i] = vote.getTransactionID();
        }

        // -> send global decision to replicas
        for (int i = 0; i < this.w; i++) {
            futures[i] = pool.submit(globalVote(this.stubs[shuffleArray[i]], result, transactionIDs[i]));
        }

        // <- wait for ack
        for (int i = 0; i < this.w; i++) {
            futures[i].get();
        }

        return result == COMMIT;
    }

    private Value read(String k) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(this.r);
        Future<Value>[] futures = new Future[this.r];
        int[] shuffleArray = generateShuffleArray(this.stubs.length, this.w);
        // -> send read
        for (int i = 0; i < this.r; i++) {
            futures[i] = pool.submit(this.read(this.stubs[shuffleArray[i]], k));
        }

        Value mostRecent = null;
        // <- wait for results
        for (int i = 0; i < this.r; i++) {
            Value value = futures[i].get();
            if (mostRecent == null || (value != null &&
                    value.getVersioning().greaterThan(mostRecent.getVersioning()))) {
                mostRecent = value;
            }
        }
        return mostRecent;
    }

    private Callable<Value> read(Replica stub, String key) {
        return () -> stub.Read(key);
    }

    private Callable<VoteMessage> prepare(Replica stub, String key, Value value) {
        return () -> stub.Prepare(key, value);
    }

    private Callable<VoteMessage> globalVote(Replica stub, VoteMessage.MessageType result, String transactionID) {
        return () -> {
            if (result == COMMIT) {
                stub.Commit(transactionID);
            } else {
                stub.Abort(transactionID);
            }
            return null;
        };
    }

    private int[] generateShuffleArray(int n, int wr) {
        int[] array = new int[wr];
        Set<Integer> chosenNumbers = new HashSet<>();
        Random random = new Random();
        int num, i = 0;
        while (i < wr) {
            num = random.nextInt(n);
            if (chosenNumbers.add(num)) {
                array[i++] = num;
            }
        }
        return array;
    }
}
