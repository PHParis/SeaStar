package de.unikoblenz.west.seastar.launcher;

import de.unikoblenz.west.seastar.datapreparation.Graph2DatasetMatcher;
import de.unikoblenz.west.seastar.datapreparation.LDCrawlLinksImporter;
import de.unikoblenz.west.seastar.model.Dataset;
import de.unikoblenz.west.seastar.model.TypeOfDatasetLocation;
import de.unikoblenz.west.seastar.utils.ConfigurationManager;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by csarasua.
 */
public class Main {

    static String workingDir= System.getProperty("user.dir");
    static String workingDirForFileName= workingDir.replace("\\", "/");

    private static final Logger log= LogManager.getLogger(Main.class);


    public static void main(String[] args) {




       


        String name = args[0];
        String linksFileName = args[1];
        String dataFileName = args[2];
        String namespace = args[3];
        String typelinks = args[4];
        analyzeLinks(name, linksFileName, dataFileName, namespace,typelinks);



       
    }


}

