package it.polimi.ds;

public class Transaction {
    
    private String transactionID;
    private String key;
    private Value value;
    
    public Transaction(String transactionID, String key, Value value) {
        this.transactionID = transactionID;
        this.key = key;
        this.value = value;
    }
    
    public void setTransactionID(String transactionID) {
        this.transactionID = transactionID;
    }
    
    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(Value value) {
        this.value = value;
    }

    public String getTransactionID() {
        return transactionID;
    }

    public String getKey() {
        return key;
    }
    
    public Value getValue() {
        return value;
    }
    
}
