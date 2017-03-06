package de.unikoblenz.west.seastar.datapreparation;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.*;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created by csarasua.
 */
public class DatasetSplitter {

    String inputData;
    String catalogueFilePath;
    String ls = System.getProperty("line.separator");


    Connection connect;

    // PLD, FileName
    Map<String,String> datasets = new HashMap<String,String>();

    public DatasetSplitter(String inputData, String pathCatalogue)
    {

        this.catalogueFilePath = pathCatalogue;
        this.inputData = inputData;

       // loadDatasetCatalogue(); do now one file for each context n in the n-quad

    }

    private void loadDatasetCatalogue() {

        Reader in = null;
        try {
            in = new FileReader(this.catalogueFilePath);
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




            String datasetPLD = record.get("dataset");
            // log.info("PLD of data set: " + datasetPLD);

            //String category = record.get("category");
            // log.info("LOD topical domain of data set: " + category);


            this.datasets.put(datasetPLD, "dataset_"+datasetPLD+".nt" );





        }


    }

    public void splitDatasets()
    {
        GZIPInputStream in=null;

        try {
            in = new GZIPInputStream(new FileInputStream(this.inputData));
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
            String nameFileT = context.replace("/", "");
            String nameFileTT = nameFileT.replace("http:","");
            String nameFile = nameFileTT.replace(".","-");

            File f = new File("/home/ubuntu/seastardatasets/dataset_"+nameFile+".nt");

            try {
                Files.append("<"+subject+"> <"+predicate+"> <"+object+"> .",f,Charsets.UTF_8);
                Files.append(ls, f, Charsets.UTF_8);
            } catch (IOException e) {
                e.printStackTrace();
            }


//Files.write("", new File(this.outputFilePath), Charsets.UTF_8);

    }




}


}
