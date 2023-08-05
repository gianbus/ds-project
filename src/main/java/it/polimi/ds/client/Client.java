package it.polimi.ds.client;

import it.polimi.ds.rmi.RemoteInfo;

public class Client {
    private Client() {}

    public static void main(String[] args) {
        try {
            String initialHost = (args.length < 1) ? null : args[0];
            if(initialHost == null) throw new IllegalArgumentException("You neeed to specify a host!");

            String initialRegistryName = (args.length < 2) ? null : args[1];
            if(initialRegistryName == null) throw new IllegalArgumentException("You neeed to specify a registry name!");

            RemoteInfo initialRemoteInfo = new RemoteInfo(initialHost, initialRegistryName);

            Middleware middleware = new LeaderlessMiddleware(new DefaultConnector(), initialRemoteInfo);
            
            String v = middleware.Get("a");
            middleware.Put("b", "hello");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}