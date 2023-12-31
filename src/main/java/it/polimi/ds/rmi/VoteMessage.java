package it.polimi.ds.rmi;

import java.io.Serializable;

public class VoteMessage implements Serializable{
    public enum MessageType {
        COMMIT,
        ABORT
    }

    private final MessageType type;
    private final String transactionID;

    public VoteMessage(MessageType type, String transactionID) {
        this.type = type;
        this.transactionID = transactionID;
    }

    public MessageType getType() {
        return this.type;
    }

    public String getTransactionID() {
        return this.transactionID;
    }
}
