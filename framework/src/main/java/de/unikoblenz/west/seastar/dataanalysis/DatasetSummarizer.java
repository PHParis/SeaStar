package de.unikoblenz.west.seastar.dataanalysis;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import de.unikoblenz.west.seastar.model.Dataset;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;


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
    ArrayList<Double> classifDiffEntropyRatio = new ArrayList<Double>();
    ArrayList<Double> descmDiffEntropy = new ArrayList<Double>();
    ArrayList<Double> descmDiffEntropyRatio = new ArrayList<Double>();
    ArrayList<Double> descmPDiffEntropy = new ArrayList<Double>();
    ArrayList<Double> descmPDiffEntropyRatio = new ArrayList<Double>();

    ArrayList<Double> econnDiffEntropy = new ArrayList<Double>();
    ArrayList<Double> econnDiffEntropyRatio = new ArrayList<Double>();
    ArrayList<Double> dconnDiffEntropy = new ArrayList<Double>();
    ArrayList<Double> dconnDiffEntropyRatio = new ArrayList<Double>();

    ArrayList<Double> vocabDescDiffEntropy  = new ArrayList<Double>();
    ArrayList<Double> vocabDescDiffEntropyRatio  = new ArrayList<Double>();

    String typelinks;

    private static final Logger log = LogManager.getLogger(DatasetSummarizer.class);

    File summaryFile;


    public DatasetSummarizer(Dataset d,String typelinks)
    {

        this.dataset=d;

        workingDir = System.getProperty("user.dir");
        workingDirForFileName = workingDir.replace("\\", "/");

        this.typelinks=typelinks;




        summaryFile = new File(workingDirForFileName + "/output/summary_"+ dataset.getTitle()+"_"+typelinks+".tsv");

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

       //TODO: add to process Nentropy and entropy
        this.classifDiffEntropyRatio = processFile(workingDirForFileName+"/output/m11bFile_"+ dataset.getTitle()+"_"+typelinks+".tsv", "diffratio");
        this.classifDiffEntropy = processFile(workingDirForFileName+"/output/m11cFile_"+ dataset.getTitle()+"_"+typelinks+".tsv", "diffentropy");

        this.descmDiffEntropyRatio = processFile(workingDirForFileName+"/output/m12bFile_"+ dataset.getTitle()+"_"+typelinks+".tsv", "diffratio");
        this.descmDiffEntropy = processFile(workingDirForFileName+"/output/m12cFile_"+ dataset.getTitle()+"_"+typelinks+".tsv", "diffentropy");

        this.descmPDiffEntropyRatio = processFile(workingDirForFileName+"/output/m13bFile_"+ dataset.getTitle()+"_"+typelinks+".tsv", "diffratio");
        this.descmPDiffEntropy = processFile(workingDirForFileName+"/output/m13cFile_"+ dataset.getTitle()+"_"+typelinks+".tsv", "diffentropy");


        this.econnDiffEntropyRatio =processFile(workingDirForFileName+"/output/m21bFile_"+ dataset.getTitle()+"_"+typelinks+".tsv", "diffratio");
        this.econnDiffEntropy =processFile(workingDirForFileName+"/output/m21cFile_"+ dataset.getTitle()+"_"+typelinks+".tsv", "diffentropy");

        this.dconnDiffEntropyRatio =processFile(workingDirForFileName+"/output/m22bFile_"+ dataset.getTitle()+"_"+typelinks+".tsv", "diffratio");
        this.dconnDiffEntropy =processFile(workingDirForFileName+"/output/m22cFile_"+ dataset.getTitle()+"_"+typelinks+".tsv", "diffentropy");


        this.vocabDescDiffEntropyRatio =processFile(workingDirForFileName+"/output/m31bFile_"+ dataset.getTitle()+"_"+typelinks+".tsv", "diffratio");
        this.vocabDescDiffEntropy =processFile(workingDirForFileName+"/output/m31cFile_"+ dataset.getTitle()+"_"+typelinks+".tsv", "diffentropy");




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
        Double classifDiffEntropyRatioAVG = calculateAverage(this.classifDiffEntropyRatio);

        Double descmDiffEntropyAVG = calculateAverage(this.descmDiffEntropy);
        Double descmDiffEntropyRatioAVG = calculateAverage(this.descmDiffEntropyRatio);

        Double descmPDiffEntropyAVG = calculateAverage(this.descmDiffEntropy);
        Double descmPDiffEntropyRatioAVG = calculateAverage(this.descmDiffEntropyRatio);



        Double econnDiffEntropyAVG = calculateAverage(this.econnDiffEntropy);
        Double econnDiffEntropyRatioAVG = calculateAverage(this.econnDiffEntropyRatio);
        Double dconnDiffEntropyAVG = calculateAverage(this.dconnDiffEntropy);
        Double dconnDiffEntropyRatioAVG = calculateAverage(this.dconnDiffEntropyRatio);


        Double vocabDescDiffEntropyAVG = calculateAverage(this.vocabDescDiffEntropy);
        Double vocabDescDiffEntropyRatioAVG = calculateAverage(this.vocabDescDiffEntropyRatio);




        Double classifDiffEntropyGini = calculateGini(this.classifDiffEntropy);
        Double classifDiffEntropyRatioGini = calculateGini(this.classifDiffEntropyRatio);
        Double descmDiffEntropyGini = calculateGini(this.descmDiffEntropy);
        Double descmDiffEntropyRatioGini = calculateGini(this.descmDiffEntropyRatio);
        Double descmPDiffEntropyGini = calculateGini(this.descmDiffEntropy);
        Double descmPDiffEntropyRatioGini = calculateGini(this.descmDiffEntropyRatio);



        Double econnDiffEntropyGini = calculateGini(this.econnDiffEntropy);
        Double econnDiffEntropyRatioGini = calculateGini(this.econnDiffEntropyRatio);

        Double dconnDiffEntropyGini = calculateGini(this.dconnDiffEntropy);
        Double dconnDiffEntropyRatioGini = calculateGini(this.dconnDiffEntropyRatio);


        Double vocabDescDiffEntropyGini = calculateGini(this.vocabDescDiffEntropy);
        Double vocabDescDiffEntropyRatioGini = calculateGini(this.vocabDescDiffEntropyRatio);

        // Write summary in file

        try {

            String ls = System.getProperty("line.separator");

            Files.append("m1.1.b"+"\t"+classifDiffEntropyRatioAVG+"\t"+classifDiffEntropyRatioGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);
            Files.append("m1.1.c"+"\t"+classifDiffEntropyAVG+"\t"+classifDiffEntropyGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);

            Files.append("m1.2.b"+"\t"+descmDiffEntropyRatioAVG+"\t"+descmDiffEntropyRatioGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);
            Files.append("m1.2.c"+"\t"+descmDiffEntropyAVG+"\t"+descmDiffEntropyGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);


            Files.append("m1.3.b"+"\t"+descmDiffEntropyRatioAVG+"\t"+descmPDiffEntropyRatioGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);
            Files.append("m1.3.c"+"\t"+descmDiffEntropyAVG+"\t"+descmPDiffEntropyGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);



            Files.append("m2.1.b"+"\t"+econnDiffEntropyRatioAVG+"\t"+econnDiffEntropyRatioGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);
            Files.append("m2.1.c"+"\t"+econnDiffEntropyAVG+"\t"+econnDiffEntropyGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);

            Files.append("m2.2.b"+"\t"+dconnDiffEntropyRatioAVG+"\t"+dconnDiffEntropyRatioGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);
            Files.append("m2.2.c"+"\t"+dconnDiffEntropyAVG+"\t"+dconnDiffEntropyGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);

            Files.append("m3.1.b"+"\t"+vocabDescDiffEntropyRatioAVG+"\t"+vocabDescDiffEntropyRatioGini+"\t", summaryFile, Charsets.UTF_8);
            Files.append(ls , summaryFile, Charsets.UTF_8);
            Files.append("m3.1.c"+"\t"+vocabDescDiffEntropyAVG+"\t"+vocabDescDiffEntropyGini+"\t", summaryFile, Charsets.UTF_8);
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

        if(size==0)
        {return 0.0;}
        else
        {
            return result;
        }



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
        Double first=0.0;
        if(di!=0.0)
        {
            first = (2.0 * idi) / (n * di);
        }
        Double second = 0.0;
        if(n!=0.0) {
         second   =(n + 1.0) / n;
        }

        gini = first - second;

        return gini;
    }


}
