package it.polimi.ds.client;

import it.polimi.ds.rmi.RemoteInfo;
import it.polimi.ds.rmi.Replica;

import java.util.HashMap;

public class MockConnector implements Connector {
    private final HashMap<RemoteInfo,Replica> replicaMap;

    public MockConnector(HashMap<RemoteInfo,Replica> replicaMap) {
        this.replicaMap = replicaMap;
    }

    @Override
    public Replica Connect(RemoteInfo remoteInfo) throws Exception {
        return replicaMap.get(remoteInfo);
    }
}
