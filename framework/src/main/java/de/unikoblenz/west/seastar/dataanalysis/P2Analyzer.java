package de.unikoblenz.west.seastar.dataanalysis;

import com.google.common.base.Charsets;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import de.unikoblenz.west.seastar.model.Dataset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.IOException;
import java.util.Map;


/**
 * Created by csarasua.
 */
public class P2Analyzer implements Analyzer {

    Dataset dataset;

    Map<String,Integer> targetMap;
    Map<String,Integer> targetMapPrime;

    Map<String,Integer> datasetMap;
    Map<String,Integer> datasetMapPrime;

    File m21aFile;
    File m21bFile;
    File m21cFile;
    File m22aFile;
    File m22bFile;
    File m22cFile;


    private static final Logger log = LogManager.getLogger(P2Analyzer.class);


    EntropyCalculator ec;

    String workingDir;
    String workingDirForFileName;


    public P2Analyzer(Dataset dataset, String typelinks)
    {

        this.dataset=dataset;
        ec = new EntropyCalculator();




        workingDir = System.getProperty("user.dir");
        workingDirForFileName = workingDir.replace("\\", "/");



        m21aFile = new File(workingDirForFileName + "/output/m21aFile_"+dataset.getTitle()+"_"+typelinks+".tsv");
        m21bFile = new File(workingDirForFileName + "/output/m21bFile_"+dataset.getTitle()+"_"+typelinks+".tsv");
        m21cFile = new File(workingDirForFileName + "/output/m21cFile_"+dataset.getTitle()+"_"+typelinks+".tsv");
        m22aFile = new File(workingDirForFileName + "/output/m22aFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");
        m22bFile = new File(workingDirForFileName + "/output/m22bFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");
        m22cFile = new File(workingDirForFileName + "/output/m22cFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");


        String ls = System.getProperty("line.separator");

        try {
            Files.write("", m21aFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" +"typeentity" + "\t"+"econncardinality" + "\t"+"econnprimecardinality", m21aFile, Charsets.UTF_8);
            Files.append(ls, m21aFile, Charsets.UTF_8);

            Files.write("", m21bFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" +"typeentity" + "\t"+"diffratio", m21bFile, Charsets.UTF_8);
            Files.append(ls, m21bFile, Charsets.UTF_8);

            Files.write("", m21cFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" +"typeentity" + "\t"+"diffentropy"+ "\t"+"diffNentropy", m21cFile, Charsets.UTF_8);
            Files.append(ls, m21cFile, Charsets.UTF_8);


            Files.write("", m22aFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" +"typeentity" + "\t"+"datasetcardinality" + "\t"+"econnprimecardinality", m22aFile, Charsets.UTF_8);
            Files.append(ls, m22aFile, Charsets.UTF_8);

            Files.write("", m22bFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" +"typeentity" + "\t"+"diffratio", m22bFile, Charsets.UTF_8);
            Files.append(ls, m22bFile, Charsets.UTF_8);

            Files.write("", m22cFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" +"typeentity" + "\t"+"diffentropy"+ "\t"+"diffNentropy", m22cFile, Charsets.UTF_8);
            Files.append(ls, m22cFile, Charsets.UTF_8);


        } catch (IOException e) {
            e.printStackTrace();
            log.error("error writing files ", e);
        }
    }

    public void analyzeLinks(String entity, String entityType)
    {
          // m21a(entity,entityType);
            m21b(entity,entityType);
          // m21c(entity,entityType);
         // m22a(entity,entityType);
            m22b(entity,entityType);
         //  m22c(entity,entityType);





    }
    private void m21a(String entity, String entityType)
    {
       int targetMapSize =  this.targetMap.size();
       int targetMapPrimeSize =  this.targetMapPrime.size();

        try {

            Files.append(entity+"\t"+entityType+"\t"+targetMapSize+"\t"+targetMapPrimeSize+"\t", m21aFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m21aFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in connectivity increase output", e);
        }


    }
    private void m21b(String entity, String entityType)
    {
        MapDifference<String,Integer> difference = Maps.difference(this.targetMap,this.targetMapPrime);
        int countNewTargets = difference.entriesOnlyOnRight().size();
        int countOriginalTargets = difference.entriesInCommon().size()+difference.entriesDiffering().size();
        double diffTargetRatio=0;
        if(countOriginalTargets!=0)
        {
            diffTargetRatio= countNewTargets / countOriginalTargets;
        }
        else
        {diffTargetRatio= 1.0;}



        try {

            Files.append(entity+"\t"+entityType+"\t"+diffTargetRatio, m21bFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m21bFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in connectivity increase output", e);
        }


    }
    private void m21c(String entity, String entityType)
    {
        double entropyTargets = ec.calculateEntropy(targetMap,true);
        double entropyTargetsPrime = ec.calculateEntropy(targetMapPrime,true);
        double diffNEntropyDataset = entropyTargetsPrime-entropyTargets;
        entropyTargets = ec.calculateEntropy(targetMap,false);
        entropyTargetsPrime = ec.calculateEntropy(targetMapPrime,false);
        double diffEntropyDataset = entropyTargetsPrime-entropyTargets;


        try {

            Files.append(entity+"\t"+entityType+"\t"+diffEntropyDataset+"\t"+diffNEntropyDataset, m21cFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m21cFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in connectivity increase output", e);
        }
    }
    private void m22a(String entity, String entityType)
    {
        int datasetMapSize  = this.datasetMap.size();
       int datasetMapPrimeSize = this.datasetMapPrime.size();

        try {

            Files.append(entity+"\t"+entityType+"\t"+datasetMapSize+"\t"+datasetMapPrimeSize+"\t", m22aFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m22aFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in connectivity increase output", e);
        }
    }

    private void m22b(String entity, String entityType)
    {
        MapDifference<String,Integer> difference = Maps.difference(this.datasetMap,this.datasetMapPrime);
        int countNewDatasets = difference.entriesOnlyOnRight().size();
        int countOriginalDatasets = difference.entriesInCommon().size()+difference.entriesDiffering().size();
        double diffDatasetsRatio=0;
        if(countOriginalDatasets!=0)
        {
            diffDatasetsRatio= countNewDatasets / countOriginalDatasets;
        }
        else
        {
            diffDatasetsRatio= 1.0;
        }


        try {

            Files.append(entity+"\t"+entityType+"\t"+diffDatasetsRatio+"\t", m22bFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m22bFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in connectivity increase output", e);
        }


    }
    private void m22c(String entity, String entityType)
    {
        double entropyDataset = ec.calculateEntropy(datasetMap,true);
        double entropyDatasetPrime = ec.calculateEntropy(datasetMapPrime,true);
        double diffNEntropyDataset = entropyDatasetPrime-entropyDataset;
        entropyDataset = ec.calculateEntropy(datasetMap,false);
        entropyDatasetPrime = ec.calculateEntropy(datasetMapPrime,false);
        double diffEntropyDataset = entropyDatasetPrime-entropyDataset;

        try {

            Files.append(entity+"\t"+entityType+"\t"+diffEntropyDataset+"\t"+diffNEntropyDataset, m22cFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m22cFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in connectivity increase output", e);
        }

    }
    private void m23(String entity, String entityType)
    {

    }

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public Map<String, Integer> getTargetMap() {
        return targetMap;
    }

    public void setTargetMap(Map<String, Integer> targetMap) {
        this.targetMap = targetMap;
    }

    public Map<String, Integer> getTargetMapPrime() {
        return targetMapPrime;
    }

    public void setTargetMapPrime(Map<String, Integer> targetMapPrime) {
        this.targetMapPrime = targetMapPrime;
    }

    public Map<String, Integer> getDatasetMap() {
        return datasetMap;
    }

    public void setDatasetMap(Map<String, Integer> datasetMap) {
        this.datasetMap = datasetMap;
    }

    public Map<String, Integer> getDatasetMapPrime() {
        return datasetMapPrime;
    }

    public void setDatasetMapPrime(Map<String, Integer> datasetMapPrime) {
        this.datasetMapPrime = datasetMapPrime;
    }

    public File getM21aFile() {
        return m21aFile;
    }

    public void setM21aFile(File m21aFile) {
        this.m21aFile = m21aFile;
    }

    public File getM21bFile() {
        return m21bFile;
    }

    public void setM21bFile(File m21bFile) {
        this.m21bFile = m21bFile;
    }

    public File getM22aFile() {
        return m22aFile;
    }

    public void setM22aFile(File m22aFile) {
        this.m22aFile = m22aFile;
    }

    public File getM22bFile() {
        return m22bFile;
    }

    public void setM22bFile(File m22bFile) {
        this.m22bFile = m22bFile;
    }


}
