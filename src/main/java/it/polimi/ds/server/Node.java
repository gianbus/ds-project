package it.polimi.ds.server;

import it.polimi.ds.Transaction;
import it.polimi.ds.Value;
import it.polimi.ds.rmi.Replica;
import it.polimi.ds.rmi.VoteMessage;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.UUID;

import static it.polimi.ds.rmi.VoteMessage.MessageType.ABORT;
import static it.polimi.ds.rmi.VoteMessage.MessageType.COMMIT;

public class Node implements Replica {
    public Node() {}

    private final HashMap<String, Value> data = new HashMap<>();
    private final HashMap<String, Transaction> transactionsById = new HashMap<>();
    private final HashMap<String, Transaction> transactionsByKey = new HashMap<>();

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
    
    public synchronized Value Read(String key){
        if (key == null) {
            throw new IllegalArgumentException();
        }
        return data.get(key);
    }

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

    public synchronized void Commit(String transactionID){
        Transaction t = this.transactionsById.get(transactionID);
        if (t != null) {
            data.put(t.getKey(), t.getValue());
            removeTransaction(transactionID);
        }
    }

    public synchronized void Abort(String transactionID){
        removeTransaction(transactionID);
    }
        
    public static void main(String[] args) {
        try {
            String registryName = (args.length < 1) ? null : args[0];
            assert registryName != null : "You neeed to specify a registryName!";
            Node node = new Node();
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