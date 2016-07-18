package de.unikoblenz.west.seastar.controller.dataanalysis;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import de.unikoblenz.west.seastar.model.Dataset;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by csarasua.
 */
public class DatasetSummarizer {


    // load the files to read the concrete measuremnts
    //in DB future

    Dataset dataset;
    String workingDir;
    String workingDirForFileName;

    ArrayList<Double> classifDiffEntropy = new ArrayList<Double>();
    ArrayList<Double> descmDiffEntropy = new ArrayList<Double>();

    ArrayList<Double> econnDiffEntropy = new ArrayList<Double>();
    ArrayList<Double> dconnDiffEntropy = new ArrayList<Double>();

    ArrayList<Double> vocabDescDiffEntropy  = new ArrayList<Double>();

    private static final Logger log = LoggerFactory.getLogger(DatasetSummarizer.class);

    File summaryFile;


    public DatasetSummarizer(Dataset d)
    {

        this.dataset=d;

        workingDir = System.getProperty("user.dir");
        workingDirForFileName = workingDir.replace("\\", "/");


        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();
        java.util.logging.Logger.getLogger("global").setLevel(Level.WARNING);

        Marker m = MarkerFactory.getMarker("debug");
        log.isDebugEnabled(m);

        summaryFile = new File(workingDirForFileName + "/output/summary_"+ dataset.getTitle()+".tsv");

        String ls = System.getProperty("line.separator");
        try {

            Files.write("", summaryFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("Measure" + "\t" + "Average" + "\t" + "GiniCoefficient" + "\t", summaryFile, Charsets.UTF_8);
            Files.append(ls, summaryFile, Charsets.UTF_8);


        } catch (IOException e) {
            e.printStackTrace();
            log.error("error writing files ", e);
        }
    }
    public void loadMeasurementsFromFiles()
    {

        this.classifDiffEntropy = processFile(workingDirForFileName+"/output/m11bFile_"+ dataset.getTitle()+".tsv", "diffentropy");
        this.descmDiffEntropy = processFile(workingDirForFileName+"/output/m12bFile_"+ dataset.getTitle()+".tsv", "diffentropy");

        this.econnDiffEntropy =processFile(workingDirForFileName+"/output/m21bFile_"+ dataset.getTitle()+".tsv", "diffentropy");
        this.dconnDiffEntropy =processFile(workingDirForFileName+"/output/m22bFile_"+ dataset.getTitle()+".tsv", "diffentropy");

        this.vocabDescDiffEntropy =processFile(workingDirForFileName+"/output/m31bFile_"+ dataset.getTitle()+".tsv", "diffentropy");



    }

    private ArrayList<Double> processFile(String filePath, String headerToSummarize )
    // several
    {

        ArrayList<Double> result= new ArrayList<Double>();
        Reader in = null;
        try {
            in = new FileReader(filePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Iterable<CSVRecord> records = null;
        try {

            // records = CSVFormat.DEFAULT.withHeader("source","predicate","target","datasets","datasett","datasetspld","datasettpld").parse(in);
            // records = CSVFormat.TDF.withDelimiter('\t').withHeader().parse(in);

            //.withHeader("entity","typeentity","diffentropy"
            records = CSVFormat.TDF.withDelimiter('\t').parse(in);

        } catch (IOException e) {
            e.printStackTrace();
        }





//TODO: finish add on
        int count=1;
        for (CSVRecord record : records) {

        //"diffentropy"
            if(count!=1) {
            Double measurement = new Double(record.get(2));

                result.add(measurement);
            }
            count++;



        }

        Collections.sort(result);
        return result;




        }

    public void analyzeDataset()
    {


        Double classifDiffEntropyAVG = calculateAverage(this.classifDiffEntropy);
        Double descmDiffEntropyAVG = calculateAverage(this.descmDiffEntropy);


        Double econnDiffEntropyAVG = calculateAverage(this.econnDiffEntropy);
        Double dconnDiffEntropyAVG = calculateAverage(this.dconnDiffEntropy);

        Double vocabDescDiffEntropyAVG = calculateAverage(this.vocabDescDiffEntropy);



        Double classifDiffEntropyGini = calculateGini(this.classifDiffEntropy);
        Double descmDiffEntropyGini = calculateGini(this.descmDiffEntropy);


        Double econnDiffEntropyGini = calculateGini(this.econnDiffEntropy);
        Double dconnDiffEntropyGini = calculateGini(this.dconnDiffEntropy);

        Double vocabDescDiffEntropyGini = calculateGini(this.vocabDescDiffEntropy);
        // Write summary in file

        try {

            String ls = System.getProperty("line.separator");

            Files.append("m1.1.b"+"\t"+classifDiffEntropyAVG+"\t"+classifDiffEntropyGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);

            Files.append("m1.2.b"+"\t"+descmDiffEntropyAVG+"\t"+descmDiffEntropyGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);


            Files.append("m2.1.b"+"\t"+econnDiffEntropyAVG+"\t"+econnDiffEntropyGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);

            Files.append("m2.2.b"+"\t"+dconnDiffEntropyAVG+"\t"+dconnDiffEntropyGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);


            Files.append("m3.1.b"+"\t"+vocabDescDiffEntropyAVG+"\t"+vocabDescDiffEntropyGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);

        } catch (IOException e) {
            log.error("problem when writing in description increase output", e);
        }

    }

    private Double calculateAverage(ArrayList<Double> listOfMeasurements)
    {
        Double result = 0.0;
        Double total = 0.0;

        for(Double d: listOfMeasurements)
        {
            total = total+d;
        }
        Double size = new Double(listOfMeasurements.size());
        result =total / size;


        return result;
    }

    private Double calculateGini(ArrayList<Double> listOfMeasurements)
    {
        Double gini = 0.0;
        Double idi = 0.0;
        Double di = 0.0;

        for(int i=0; i<listOfMeasurements.size();i++)
        {
            idi = idi + ((i+1)*listOfMeasurements.get(i));
            di=di+listOfMeasurements.get(i);

        }


        Double n = new Double(listOfMeasurements.size());
        Double first = (2.0* idi) / (n*di);
        Double second = (n +1.0) / n;

        gini = first - second;

        return gini;
    }


}
