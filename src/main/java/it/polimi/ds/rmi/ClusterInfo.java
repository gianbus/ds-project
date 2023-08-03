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

    public RemoteInfo[] getRemoteInfos() {
        return remoteInfos;
    }

    public int getR() {
        return r;
    }

    public int getW() {
        return w;
    }

    //dummyConstructor
    public ClusterInfo(){
        this.r = -1;
        this.w = -1;
        this.remoteInfos = null;
    }
}
