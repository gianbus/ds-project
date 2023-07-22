package it.polimi.ds.client;

public class StubInfo {
    private final String host;
    private final String registryName;

    public StubInfo(String host, String registryName) {
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
