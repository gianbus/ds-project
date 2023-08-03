package it.polimi.ds.client;

import it.polimi.ds.Value;
import it.polimi.ds.rmi.ClusterInfo;
import it.polimi.ds.rmi.RemoteInfo;
import it.polimi.ds.rmi.Replica;
import it.polimi.ds.rmi.VoteMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static it.polimi.ds.rmi.VoteMessage.MessageType.ABORT;
import static it.polimi.ds.rmi.VoteMessage.MessageType.COMMIT;

public class LeaderlessMiddleware implements Middleware {
    private final Function<Integer, ExecutorService> poolFactory;
    private final ArrayList<Replica> stubs;
    private final int r;
    private final int w;

    public LeaderlessMiddleware(Connector connector, RemoteInfo initialRemoteInfo) throws Exception {
        this(connector, initialRemoteInfo, Executors::newFixedThreadPool);
    }

    public LeaderlessMiddleware(Connector connector, RemoteInfo initialRemoteInfo, Function<Integer, ExecutorService> poolFactory) throws Exception {
        this.poolFactory = poolFactory;
        Replica initialReplica = connector.Connect(initialRemoteInfo);
        ClusterInfo clusterInfo = initialReplica.GetClusterInfo();
        RemoteInfo[] remoteInfos = clusterInfo.getRemoteInfos();
        this.stubs = new ArrayList<>(remoteInfos.length);
        for (RemoteInfo remoteInfo : remoteInfos) {
            this.stubs.add(connector.Connect(remoteInfo));
        }
        this.r = clusterInfo.getR();
        this.w = clusterInfo.getW();
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

        Collections.shuffle(this.stubs);
        ExecutorService pool = this.poolFactory.apply(this.w);
        Future<VoteMessage>[] futures = new Future[this.w];

        // -> send prepare to selected replicas
        for (int i = 0; i < this.w; i++) {
            futures[i] = pool.submit(this.prepare(this.stubs.get(i), k, value));
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
            futures[i] = pool.submit(globalVote(this.stubs.get(i), result, transactionIDs[i]));
        }

        // <- wait for ack
        for (int i = 0; i < this.w; i++) {
            futures[i].get();
        }

        return result == COMMIT;
    }

    private Value read(String k) throws Exception {
        Collections.shuffle(this.stubs);
        ExecutorService pool = this.poolFactory.apply(this.r);
        Future<Value>[] futures = new Future[this.r];
        // -> send read
        for (int i = 0; i < this.r; i++) {
            futures[i] = pool.submit(this.read(this.stubs.get(i), k));
        }

        Value[] values = new Value[this.r];
        Value mostRecent = null;
        // <- wait for results
        for (int i = 0; i < this.r; i++) {
            values[i] = futures[i].get();
            if (mostRecent == null || (values[i] != null &&
                    values[i].getVersioning().greaterThan(mostRecent.getVersioning()))) {
                mostRecent = values[i];
            }
        }

        if (mostRecent != null) {
            // -> repair
            for (int i = 0; i < this.r; i++) {
                if (values[i] == null || mostRecent.getVersioning().greaterThan(values[i].getVersioning())) {
                    pool.submit(this.repair(this.stubs.get(i), k, mostRecent));
                }
            }
            // no need to wait for results
        }

        return mostRecent;
    }

    private Callable<Value> read(Replica stub, String key) {
        return () -> stub.Read(key);
    }

    private Callable<Value> repair(Replica stub, String key, Value value) {
        return () -> {
            stub.Repair(key, value);
            return null;
        };
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
}
