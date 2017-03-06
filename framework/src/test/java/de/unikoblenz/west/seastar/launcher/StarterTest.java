package de.unikoblenz.west.seastar.launcher;

import de.unikoblenz.west.seastar.model.TypeOfDatasetLocation;
import org.junit.Test;

/**
 * Created by csarasua.
 */
public class StarterTest {

    String workingDir= System.getProperty("user.dir");
    String workingDirForFileName= workingDir.replace("\\", "/");

   // @Test // 50 entities at OpenEi
    public void testAEMET()
    {
        Starter starter = new Starter();
        ///resources/aemet_links.csv
        starter.startLinkAnalysis(workingDir+"/data/aemet_links.csv", "aemet", "http://aemet.linkeddata.es/sparql", TypeOfDatasetLocation.SPARQLENDPOINT,null);

    }

   //@Test
    public void testDataSemanticWeb()
    {
        Starter starter = new Starter();

        starter.startLinkAnalysis(workingDir+"/data/datasemanticweb_links.csv", "data.semanticweb", "http://data.semanticweb.org/sparql", TypeOfDatasetLocation.SPARQLENDPOINT,null);

    }

  @Test
    public void testZBW()
    {
        Starter starter = new Starter();

        //zbw.eu/stw"
        starter.startLinkAnalysis(workingDir+"/data/zbw_links.csv", "zbw", "/data/stw.rdf", TypeOfDatasetLocation.FILEDUMP,null);

    }
   // @Test
    public void testReegle()
    {
        Starter starter = new Starter();

        //zbw.eu/stw"
        starter.startLinkAnalysis(workingDir+"/data/reegle_links.csv", "reegle", "http://sparql.reeep.org/", TypeOfDatasetLocation.SPARQLENDPOINT,null);

    }


    /*
    @Test
    public void testDataSemanticWeb()
    {
        Starter starter = new Starter();

        starter.startLinkAnalysis(workingDir+"/data/datasw_links.csv", "data.semanticweb", "http://data.semanticweb.org/snorql/", TypeOfDatasetLocation.SPARQLENDPOINT,null);

    }

    @Test
    public void testDataSemanticWeb()
    {
        Starter starter = new Starter();

        starter.startLinkAnalysis(workingDir+"/data/datasw_links.csv", "data.semanticweb", "http://data.semanticweb.org/snorql/", TypeOfDatasetLocation.SPARQLENDPOINT,null);

    }
    */



    // 50 entities at


    // 50 entities at

    // 50 entities at



    // Repeat for many others
}