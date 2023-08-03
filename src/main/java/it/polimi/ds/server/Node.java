package it.polimi.ds.server;

import it.polimi.ds.Transaction;
import it.polimi.ds.Value;
import it.polimi.ds.rmi.ClusterInfo;
import it.polimi.ds.rmi.Replica;
import it.polimi.ds.rmi.VoteMessage;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.UUID;

import static it.polimi.ds.rmi.VoteMessage.MessageType.ABORT;
import static it.polimi.ds.rmi.VoteMessage.MessageType.COMMIT;

public class Node implements Replica {
    private final ClusterInfo clusterInfo;
    private final HashMap<String, Value> data = new HashMap<>();
    private final HashMap<String, Transaction> transactionsById = new HashMap<>();
    private final HashMap<String, Transaction> transactionsByKey = new HashMap<>();

    public Node(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }

    private synchronized void recordTransaction(Transaction t){
        this.transactionsById.put(t.getTransactionID(), t);
        this.transactionsByKey.put(t.getKey(), t);
    }

    private synchronized void removeTransaction(String transactionID){
        Transaction t = this.transactionsById.get(transactionID);
        if (t != null) {
            this.transactionsById.remove(transactionID);
            this.transactionsByKey.remove(t.getKey());
        }
    }

    @Override
    public ClusterInfo GetClusterInfo() throws RemoteException {
        return this.clusterInfo;
    }

    @Override
    public synchronized Value Read(String key){
        if (key == null) {
            throw new IllegalArgumentException();
        }
        return data.get(key);
    }

    @Override
    public synchronized void Repair(String key, Value value) throws RemoteException {
        if (key == null || value == null) {
            throw new IllegalArgumentException();
        }
        Value currentValue = data.get(key);
        if (currentValue == null || value.getVersioning().greaterThan(currentValue.getVersioning())) {
            data.put(key, value);
        }
    }

    @Override
    public synchronized VoteMessage Prepare(String key, Value value){
        if (key == null || value == null) {
            throw new IllegalArgumentException();
        }

        String transactionID = UUID.randomUUID().toString();
        while (this.transactionsById.containsKey(transactionID)) {
            transactionID = UUID.randomUUID().toString();
        }
        Transaction t = new Transaction(transactionID ,key, value);

        Value currentValue = data.get(key);
        if(transactionsByKey.get(key) == null){
            if (currentValue == null || value.getVersioning().greaterThan(currentValue.getVersioning())) {
                recordTransaction(t);
                return new VoteMessage(COMMIT, transactionID);
            }
        }

        return new VoteMessage(ABORT, transactionID);
    }

    @Override
    public synchronized void Commit(String transactionID){
        Transaction t = this.transactionsById.get(transactionID);
        if (t != null) {
            data.put(t.getKey(), t.getValue());
            removeTransaction(transactionID);
        }
    }

    @Override
    public synchronized void Abort(String transactionID){
        removeTransaction(transactionID);
    }
        
    public static void main(String[] args) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.setVisibilityChecker(VisibilityChecker.Std.defaultInstance().withFieldVisibility(JsonAutoDetect.Visibility.ANY));
            
            String configurationPath = (args.length < 1) ? null : args[0];
            assert configurationPath != null : "You neeed to specify a configuration path!";
            
            ClusterInfo clusterInfo = mapper.readValue(new File(configurationPath), ClusterInfo.class);

            Node node = new Node(clusterInfo);
            Replica stub = (Replica) UnicastRemoteObject.exportObject(node, 0);

            //Binding the stub
            Registry registry = LocateRegistry.getRegistry();
            registry.bind(registryName, stub);

            System.err.println("Node ready");
        } catch (Exception e) {
            System.err.println("Node Exception: " + e);
            e.printStackTrace();
        }
    }
}