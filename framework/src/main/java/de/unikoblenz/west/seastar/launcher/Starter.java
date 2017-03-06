package de.unikoblenz.west.seastar.launcher;

import de.unikoblenz.west.seastar.dataanalysis.*;
import de.unikoblenz.west.seastar.model.Dataset;
import de.unikoblenz.west.seastar.model.TypeOfDatasetLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Created by csarasua.
 */
public class Starter {

    // In case of multiple configurations (like DirectLInks, Cartesian etc)
    private static final Logger log = LogManager.getLogger(Starter.class);



    public Starter() {

        log.info("in starter");

    }


    public void startLinkAnalysis(String linksFilePath, String datasetName, String datasetLocation, TypeOfDatasetLocation locationType, String namespace, String typelinks) {

        Dataset d = new Dataset(datasetName,locationType,datasetLocation,null,null,namespace);


        // Stats analyzer

        DescriptiveStatsAnalyzer stats = new DescriptiveStatsAnalyzer(d,typelinks);

        stats.computeStats(d.getTitle(), namespace);
        stats.closeOpenConnections();


        Set<RawStatsAnalyzer> setOfStatsAnalyzers = new HashSet<RawStatsAnalyzer>();




        List<Analyzer> setOfPrincipleAnalyzers = new ArrayList<Analyzer>();

        File f = new File(linksFilePath);


        P1Analyzer p1Analyzer = new P1Analyzer(d,typelinks);
        setOfPrincipleAnalyzers.add(p1Analyzer);
        P2Analyzer p2Analyzer = new P2Analyzer(d, typelinks);
        setOfPrincipleAnalyzers.add(p2Analyzer);
        P3Analyzer p3Analyzer = new P3Analyzer(d,typelinks);
        setOfPrincipleAnalyzers.add(p3Analyzer);

        SeaStarAnalyzer starfish = new SeaStarAnalyzer(d,linksFilePath, setOfStatsAnalyzers, setOfPrincipleAnalyzers);
        starfish.processLinks();

        // TODO - ad measurements to db

        DatasetSummarizer summ = new DatasetSummarizer(d,typelinks);
        summ.loadMeasurementsFromFiles();;
        summ.analyzeDataset();

     




}
