package de.unikoblenz.west.seastar.dataanalysis;

/**
 * Created by csarasua.
 */
public interface RawStatsAnalyzer {

    //datasetPLD
    public void computeStats(String datasetName,  String namespace);
    public void closeOpenConnections();
}
