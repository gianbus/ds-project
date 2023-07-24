package it.polimi.ds.client;

import it.polimi.ds.rmi.Replica;

public interface Connector {
    Replica Connect(RemoteInfo remoteInfo) throws Exception;
}
