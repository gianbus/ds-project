package it.polimi.ds.client;

public interface Middleware {
    String Get(String k) throws Exception;
    boolean Put(String k, String v) throws Exception;
}
