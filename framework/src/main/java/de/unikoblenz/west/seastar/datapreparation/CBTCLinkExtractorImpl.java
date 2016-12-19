package de.unikoblenz.west.seastar.datapreparation;

import com.google.common.base.Charsets;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import com.google.common.io.Files;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import java.util.zip.GZIPInputStream;

/**
 * Created by csarasua.
 * <p>
 * Extracts all (external) links from a set of RDF triples, conformant with the Billion Triple Challenge collection procedure (e.g. BTC and MannheimCrawl follow this procedure).
 */
public class CBTCLinkExtractorImpl implements LinkExtractor {

    String filePath;
    String cataloguePath;

    String outputFilePath;
    File output;
    String erroneousURIsFilePath;
    File errors;
    String predicatesFilePath;
    File predicates;


    String workingDir = System.getProperty("user.dir");

    HashMap<String, String> datasets = new HashMap<String, String>();

    private static final Logger log = LogManager.getLogger(CBTCLinkExtractorImpl.class);

    /**
     *
     * @param pathD path of file with the data set
     * @param pathC path of file with the catalogie info (data set pay level domains and names)
     * @param pathO path of output file for links
     * @param pathE path of output file for erroneous URIs
     * @param pathP path of output file for predicates
     */
    public CBTCLinkExtractorImpl(String pathD, String pathC, String pathO, String pathE, String pathP) {


        this.filePath = pathD;
        this.cataloguePath = pathC;
        this.outputFilePath = pathO;
        this.erroneousURIsFilePath = pathE;
        this.predicatesFilePath = pathP;

        output = new File(outputFilePath);
        errors = new File(erroneousURIsFilePath);
        predicates = new File(predicatesFilePath);

        try {
            Files.write("", new File(this.outputFilePath), Charsets.UTF_8);
            Files.write("", new File(this.erroneousURIsFilePath), Charsets.UTF_8);
            Files.write("", new File(this.predicatesFilePath), Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }



    }


    private void loadDatasetCatalogue() {

        Reader in = null;
        try {
            in = new FileReader(this.cataloguePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Iterable<CSVRecord> records = null;
        try {
            records = CSVFormat.TDF.withHeader("dataset", "category").parse(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (CSVRecord record : records) {




            log.info("logging on");

            String datasetPLD = record.get("dataset");
            log.info("PLD of data set: " + datasetPLD);

            String category = record.get("category");
            log.info("LOD topical domain of data set: " + category);

            this.datasets.put(datasetPLD, category);

        }
    }


    public void extractExternalLinks() {

        loadDatasetCatalogue();
        GZIPInputStream in = null;


        try {
            in = new GZIPInputStream(new FileInputStream(this.filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }


        NxParser nxp = new NxParser();
        nxp.parse(in);

        int count = 0;

        for (Node[] nx : nxp) {


            String subject = nx[0].getLabel();
            String predicate = nx[1].getLabel();
            String object = nx[2].getLabel();
            String context = nx[3].getLabel();


            if (isLink(subject, predicate, object)) {
                try {
                    Files.append("<" + subject + "> <" + predicate + "> <" + object + "> <" + context + "> .", output, Charsets.UTF_8);
                    String ls = System.getProperty("line.separator");
                    Files.append(ls, output, Charsets.UTF_8);

                    Files.append(predicate, predicates, Charsets.UTF_8);
                    Files.append(ls, predicates, Charsets.UTF_8);

                } catch (IOException e) {
                    log.error("problem generating links.nq ",e);                }
            }

            count++;
        }

    }

    private String pld(String URI) {
        String pld = null;

        StringBuffer pldTemp = new StringBuffer();

        String[] URIparts = URI.split("/");
        int partsL = 0;

        for (int i = 0; i < URIparts.length; i++) {
            if (i != 0) {
                pldTemp.append("/");
            }
            pldTemp.append(URIparts[partsL]);

            if (this.datasets.containsKey(pldTemp.toString())) {
                pld = new String(pldTemp.toString());
                break;
            }
            partsL++;
        }
        if (pld == null) {
            pldTemp = new StringBuffer();

            pldTemp.append(URIparts[0]);
            pldTemp.append("/");
            pldTemp.append(URIparts[1]);
            pldTemp.append("/");
            pldTemp.append(URIparts[2]);
            pldTemp.append("/");
            pld = new String(pldTemp.toString());
        }


        return pld;
    }

    private boolean isLink(String subject, String predicate, String object) {

        //explore

        // subject, predicate, object

        String subjectPLD = null;
        String objectPLD = null;

        List<String> uris = new ArrayList<String>();
        uris.add(subject);
        uris.add(predicate);
        uris.add(object);

        //TODO: this repeats check unnecessarily

        for (int i = 0; i < uris.size(); i++) {

            String uri = new String(uris.get(i));

            if (uris.get(i).startsWith("http://")) // else is blank node
            {
                try {
                    URI uriCheck = new URI(uri);

                    String pldFromURI = pld(uri);

                    if (i == 0) {
                        subjectPLD = pldFromURI;
                    } else if (i == 2) {
                        objectPLD = pldFromURI;
                    }


                } catch (URISyntaxException e) {


                    try {
                        //Charset.defaultCharset()
                        Files.append(uri, errors, Charsets.UTF_8);
                        String ls = System.getProperty("line.separator");
                        Files.append(ls, errors, Charsets.UTF_8);
                    } catch (IOException e1) {
                        e1.printStackTrace();
                        //System.out.println("a");
                    }

                }
            } else {//System.out.println("no link!");
                return false;
            }

        }

        if (subjectPLD != null && objectPLD != null && !subjectPLD.equals(objectPLD)) {
            return true;
        } else {

            return false;
        }


    }

    /*
    //not needed
    public void cleanExtractedLinks() {

        //TODO: file path
        File f = new File("C:/Users/csarasua/Documents/DAIQ/src/main/resources/links2.nq");
        try {
            Files.write("", f, Charset.defaultCharset());
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileInputStream in = null;

        //TODO: file path
        try {
            in = new FileInputStream("C:/Users/csarasua/Documents/DAIQ/src/main/resources/links.nq");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


        NxParser nxp = new NxParser();
        nxp.parse(in);


        for (Node[] nx : nxp) {

            try {
                Files.append("<" + nx[0].getLabel() + "> <" + nx[1].getLabel() + "> <" + nx[2].getLabel() + "> <" + nx[3].getLabel() + "> .", f, Charsets.UTF_8);
                String ls = System.getProperty("line.separator");
                Files.append(ls, f, Charsets.UTF_8);
            } catch (IOException e) {
                e.printStackTrace();
            }


        }
    }
    */


}
