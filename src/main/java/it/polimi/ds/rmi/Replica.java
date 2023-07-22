package it.polimi.ds.rmi;

import it.polimi.ds.Value;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Replica extends Remote {
    Value Read(String key) throws RemoteException;
    VoteMessage Prepare(String key, Value value) throws RemoteException;
    void Commit(String transactionID) throws RemoteException;
    void Abort(String transactionID) throws RemoteException;
}