package de.unikoblenz.west.seastar.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by csarasua.
 */
public class URIUtils {
    static Map<String,String> datasets = new HashMap<String,String>();

    //TODO: this one should not be public?
    public static void loadDatasetCatalogue() {

        Reader in = null;
        try {
            //TODO:change path local properties
            in = new FileReader("C:/Users/csarasua/Documents/SeaStar/src/main/resources/datasetsAndCategories.tsv");
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


            Marker m = MarkerFactory.getMarker("debug");
            //log.isDebugEnabled(m);
            //log.info("logging on");

            String datasetPLD = record.get("dataset");
            // log.info("PLD of data set: " + datasetPLD);

            String category = record.get("category");
            // log.info("LOD topical domain of data set: " + category);

           datasets.put(datasetPLD, category);

        }
    }
    public static String pld(String URI) {

        String pld = null;

        try {


            StringBuffer pldTemp = new StringBuffer();

            System.out.println("URI: " + URI);
            String[] URIparts = URI.split("/");

            if (URIparts.length == 0) {
                return URI;
            }
            int partsL = 0;

            for (int i = 0; i < URIparts.length; i++) {
                if (i != 0) {
                    pldTemp.append("/");
                }
                pldTemp.append(URIparts[partsL]);

                if (datasets.containsKey(pldTemp.toString())) {
                    pld = new String(pldTemp.toString());
                    break;
                }
                partsL++;
            }
            if (pld == null) {
                pldTemp = new StringBuffer();

                if(URIparts.length>0) {
                    pldTemp.append(URIparts[0]);
                    pldTemp.append("/");
                }
                if(URIparts.length>1) {
                    pldTemp.append(URIparts[1]);
                    pldTemp.append("/");
                }
                if(URIparts.length>2) {
                    pldTemp.append(URIparts[2]);
                    pldTemp.append("/");
                }



                pld = new String(pldTemp.toString());
            }

        }
        catch(Exception e)
        {

            e.printStackTrace();
        }
        return pld;
    }

    public static String vocabulary(String URI)
    {
        //TODO: include querying LOV SPARQL endpoint
        String pld = null;

        StringBuffer pldTemp = new StringBuffer();

        System.out.println("URI: " + URI);
        String[] URIparts = URI.split("/");

        if (URIparts.length == 0)
        { return URI;}
        int partsL = 0;

        for (int i = 0; i < URIparts.length; i++) {
            if (i != 0) {
                pldTemp.append("/");
            }
            pldTemp.append(URIparts[partsL]);

            if (datasets.containsKey(pldTemp.toString())) {
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
            pldTemp.append(URIparts[3]);
            pldTemp.append("/");
            pld = new String(pldTemp.toString());
        }


        return pld;
    }

}
