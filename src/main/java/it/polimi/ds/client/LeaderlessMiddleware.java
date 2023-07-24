package it.polimi.ds.client;

import it.polimi.ds.Value;
import it.polimi.ds.rmi.Replica;
import it.polimi.ds.rmi.VoteMessage;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
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

    public LeaderlessMiddleware(StubInfo[] stubInfos, int r, int w) throws Exception {
        int n = stubInfos.length;
        if (2 * w <= n) {
            throw new RuntimeException("invalid w quorum");
        }
        if (r + w <= n) {
            throw new RuntimeException("invalid r+w quorum");
        }
        this.stubs = new Replica[n];
        for (int i = 0; i < n; i++) {
            Registry registry = LocateRegistry.getRegistry(stubInfos[i].getHost());
            this.stubs[i] = (Replica) registry.lookup(stubInfos[i].getRegistryName());
        }
        this.r = r;
        this.w = w;
    }

    @Override
    public String Get(String k) throws Exception {
        return this.read(k).getValue();
    }

    @Override
    public void Put(String k, String v) throws Exception {
        Value.Version version = new Value.Version(this.read(k).getVersion().getVersion() + 1);
        final Value value = new Value(version, v);

        VoteMessage.MessageType result = COMMIT;
        ExecutorService pool = Executors.newFixedThreadPool(this.w);
        Future<VoteMessage>[] futures = new Future[this.w];

        // -> send prepare to selected replicas
        for (int i : generateShuffleArray(this.stubs.length, this.w)) {
            futures[i] = pool.submit(this.prepare(this.stubs[i], k, value));
        }

        // <- wait for votes
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
            futures[i] = pool.submit(globalVote(this.stubs[i], result, transactionIDs[i]));
        }

        // <- wait for ack
        for (int i = 0; i < this.w; i++) {
            futures[i].get();
        }
    }

    private Value read(String key) throws Exception {
        Value mostRecent = null;

        for (int i : generateShuffleArray(this.stubs.length, this.r)) {
            Value value = this.stubs[i].Read(key);
            if (mostRecent == null || value.getVersion().greaterThan(mostRecent.getVersion())) {
                mostRecent = value;
            }
        }
        return mostRecent;
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
