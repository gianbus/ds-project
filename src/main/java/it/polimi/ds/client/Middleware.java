package it.polimi.ds.client;

public interface Middleware {
    String Get(String k) throws Exception;
    void Put(String k, String v) throws Exception;
}
