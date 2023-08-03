package it.polimi.ds.rmi;

public class ClusterInfo {
    private final RemoteInfo[] remoteInfos;
    private final int r;
    private final int w;

    public ClusterInfo(RemoteInfo[] remoteInfos, int r, int w) {
        this.remoteInfos = remoteInfos;
        this.r = r;
        this.w = w;
    }
}
