package it.polimi.ds.rmi;

import java.io.Serializable;

public class RemoteInfo implements Serializable {
    private final String host;
    private final String registryName;

    public RemoteInfo(String host, String registryName) {
        this.host = host;
        this.registryName = registryName;
    }

    public String getHost() {
        return host;
    }

    public String getRegistryName() {
        return registryName;
    }

    // empty constructor for json library
    public RemoteInfo(){
        this.host = "";
        this.registryName = "";
    }
}
