package it.polimi.ds.client;

import it.polimi.ds.rmi.RemoteInfo;
import it.polimi.ds.rmi.Replica;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class DefaultConnector implements Connector {
    @Override
    public Replica Connect(RemoteInfo remoteInfo) throws Exception {
        Registry registry = LocateRegistry.getRegistry(remoteInfo.getHost());
        return (Replica) registry.lookup(remoteInfo.getRegistryName());
    }
}
