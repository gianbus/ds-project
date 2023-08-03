package it.polimi.ds.rmi;

public class RemoteInfo {
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
}
