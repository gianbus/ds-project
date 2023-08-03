package it.polimi.ds.server;

import it.polimi.ds.Value;
import it.polimi.ds.rmi.VoteMessage;
import org.junit.Assert;
import org.junit.Test;

import static it.polimi.ds.rmi.VoteMessage.MessageType.ABORT;
import static it.polimi.ds.rmi.VoteMessage.MessageType.COMMIT;

public class TestNode {
    @Test
    public void TestCommit() {
        Node node = new Node(null);

        Value value = new Value(new Value.Versioning(1), "value");
        VoteMessage message = node.Prepare("key", value);
        Assert.assertEquals(COMMIT, message.getType());
        Assert.assertNull(node.Read("key"));

        String transactionID = message.getTransactionID();
        node.Commit(transactionID);
        Assert.assertEquals(value, node.Read("key"));
    }

    @Test
    public void TestMultipleClients() {
        Node node = new Node(null);

        Value value1 = new Value(new Value.Versioning(1), "1");
        VoteMessage message1 = node.Prepare("x", value1);
        Assert.assertEquals(COMMIT, message1.getType());

        Value value2 = new Value(new Value.Versioning(1), "2");
        VoteMessage message2 = node.Prepare("y", value2);
        Assert.assertEquals(COMMIT, message2.getType());

        Value value3 = new Value(new Value.Versioning(2), "3");
        VoteMessage message3 = node.Prepare("x", value3);
        Assert.assertEquals(ABORT, message3.getType());

        node.Commit(message2.getTransactionID());
        node.Abort(message1.getTransactionID());
        node.Abort(message3.getTransactionID());

        Assert.assertNull(node.Read("x"));
        Assert.assertEquals(value2, node.Read("y"));
    }
}
