package it.polimi.ds.server;

import it.polimi.ds.Hello;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.ServerNotActiveException;
import java.rmi.server.UnicastRemoteObject;
        
public class Node implements Replica {
    public Node() {}

    private TransactionState state = WAITING;
    private HashMap<String, Value> data = new HashMap<>();
    private HashMap<String, Transaction> transactionsById = new HashMap<>();
    private HashMap<String, Transaction> transactionsByKey = new HashMap<>();

    private void recordTransaction(Transaction t){
        synchronized(this){
            this.transactionsById.put(t.getTransactionId(), t);
            this.transactionsByKey.put(t.getKey(), t);
        }
    }

    private void removeTransaction(String transactionID){
        synchronized(this){
            Transaction t = this.transactionsById.get(transactionID);
            this.transactionsById.remove(transactionID);
            this.transactionsByKey.remove(t.getKey());
        }
    }
    
    Value Read(String key){
        synchronized(this.data){
            return data.get(key);
        }
    }

    VoteMessage Prepare(String key, Value value){

        String transactionID = UUID.randomUUID().toString();
        Transaction t = Transaction(transactionID ,key, value);

        synchronized(this){
            Value actualValue = data.get(key);
            if(transactionsByKey.get(key) == null){
                if(value.getVersion() > actualValue.getVersion() || value.getVersion() == actualValue.getVersion() && value.getTimestamp() > actualValue.getTimestamp()){
                    recordTransaction(t);
                    return VoteMessage(transactionID, COMMIT)
                }
            }
        }

        return VoteMessage(transactionID, ABORT)
    }

    void Commit(String transactionID){

        synchronized(this){
            Transaction t = this.transactionsById.get(transactionID);
            data.put(t.getKey(), t.getValue());
            removeTransaction(transactionID);
        }

    }

    void Abort(String transactionID){
        removeTransaction(transactionID);
    }
        
    public static void main(String args[]) {
        try {
            String registryName = (args.length < 1) ? null : args[0];
            assert registryName != null : "You neeed to specify a registryName!";
            Server obj = new Server();
            Replica stub = (Replica) UnicastRemoteObject.exportObject(obj, 0);

            //Binding the stub
            Registry registry = LocateRegistry.getRegistry();
            registry.bind(registryName, stub);

            System.err.println("Node ready");
        } catch (Exception e) {
            System.err.println("Node Exception: " + e.toString());
            e.printStackTrace();
        }
    }
}