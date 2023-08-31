package it.polimi.ds.server;

import it.polimi.ds.Value;
import it.polimi.ds.rmi.VoteMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

        // this will be blocked, run on a new thread
        Value value3 = new Value(new Value.Versioning(2), "3");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Callable<VoteMessage> callable = () -> node.Prepare("x", value3);
        Future<VoteMessage> future = executor.submit(callable);

        try {
            node.Commit(message2.getTransactionID());
            node.Abort(message1.getTransactionID());

            executor.shutdown();
            VoteMessage message3 = future.get();
            Assert.assertEquals(COMMIT, message2.getType());

            node.Commit(message3.getTransactionID());
        } catch (Exception e) {
            Assert.fail("unexpected exception: " + e.getMessage());
        }

        Assert.assertEquals(value3, node.Read("x"));
        Assert.assertEquals(value2, node.Read("y"));
    }
}
