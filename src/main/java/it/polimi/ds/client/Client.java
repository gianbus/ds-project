package it.polimi.ds.client;

public class Client {
    private Client() {}

    public static void main(String[] args) {
        try {
            // TODO : set appropriate values
            Middleware middleware = new LeaderlessMiddleware(null, 0, 0);
            middleware.Get("a");
            middleware.Put("b", "hello");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}