package it.polimi.ds.rmi;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class TestClusterInfo {

    @Test
    public void TestGetRegistryNames() {
        ClusterInfo clusterInfo = new ClusterInfo(new RemoteInfo[]{
            new RemoteInfo("A", "registryNameA"),
            new RemoteInfo("B", "registryNameB"),
            new RemoteInfo("C", "registryNameC")
        }, 2, 2);
        
        ArrayList<String> expectedRegistryNames = new ArrayList<>(Arrays.asList("registryNameA","registryNameB","registryNameC"));
        ArrayList<String> actualRegistryNames = clusterInfo.getRegistryNames();

        Assert.assertEquals(expectedRegistryNames, actualRegistryNames);
    }
}
