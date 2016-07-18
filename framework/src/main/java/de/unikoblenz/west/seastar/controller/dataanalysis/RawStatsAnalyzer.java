package de.unikoblenz.west.seastar.controller.dataanalysis;

/**
 * Created by csarasua.
 */
public interface RawStatsAnalyzer {

    //datasetPLD
    public void computeStats(String datasetName);
    public void closeOpenConnections();
}
