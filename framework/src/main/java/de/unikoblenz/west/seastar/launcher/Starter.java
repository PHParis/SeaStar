package de.unikoblenz.west.seastar.launcher;

import de.unikoblenz.west.seastar.controller.dataanalysis.*;
import de.unikoblenz.west.seastar.model.Dataset;
import de.unikoblenz.west.seastar.model.TypeOfDatasetLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by csarasua.
 */
public class Starter {

    // In case of multiple configurations (like DirectLInks, Cartesian etc)
    private static final Logger log = LoggerFactory.getLogger(Starter.class);



    public Starter() {
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();
        java.util.logging.Logger.getLogger("global").setLevel(Level.WARNING);
        Marker m = MarkerFactory.getMarker("debug");
        log.isDebugEnabled(m);


    }


    public void startLinkAnalysis(String linksFilePath, String datasetName, String datasetLocation, TypeOfDatasetLocation locationType, String namespace) {

        Dataset d = new Dataset(datasetName,locationType,datasetLocation,null,null,namespace);


        // Stats analyzer
        DescriptiveStatsAnalyzer stats = new DescriptiveStatsAnalyzer(d);

        Set<RawStatsAnalyzer> setOfStatsAnalyzers = new HashSet<RawStatsAnalyzer>();
        stats.computeStats(d.getTitle());
        stats.closeOpenConnections();


        List<Analyzer> setOfPrincipleAnalyzers = new ArrayList<Analyzer>();

        P1Analyzer p1Analyzer = new P1Analyzer(d);
        setOfPrincipleAnalyzers.add(p1Analyzer);
        P2Analyzer p2Analyzer = new P2Analyzer(d);
        setOfPrincipleAnalyzers.add(p2Analyzer);
        P3Analyzer p3Analyzer = new P3Analyzer(d);
        setOfPrincipleAnalyzers.add(p3Analyzer);

        SeaStarAnalyzer starfish = new SeaStarAnalyzer(d,linksFilePath, setOfStatsAnalyzers, setOfPrincipleAnalyzers);
        starfish.processLinks();

        // TODO - further db

        DatasetSummarizer summ = new DatasetSummarizer(d);
        summ.loadMeasurementsFromFiles();;
        summ.analyzeDataset();









    }


   /*
        CBTCLinkExtractorImpl linkExtractor = new CBTCLinkExtractorImpl("C:/Users/csarasua/Documents/DAIQ/src/main/resources/dump.nq.gz","C:/Users/csarasua/Documents/Starfish/src/main/resources/datasetsAndCategories.tsv",
                "C:/Users/csarasua/Documents/Starfish/src/main/resources/links.nq","C:/Users/csarasua/Documents/Starfish/src/main/resources/erroneousURIs.txt", "C:/Users/csarasua/Documents/Starfish/src/main/resources/predicates.txt");

        LDCrawlLinksImporter linkImporter= new LDCrawlLinksImporter("C:/Users/csarasua/Documents/Starfish/src/main/resources/links.zip", "C:/Users/csarasua/Documents/Starfish/src/main/resources/datasetsAndCategories.tsv", "C:/Users/csarasua/Documents/Starfish/src/main/resources/statsmannheimlinks.csv");

    }*/




}
