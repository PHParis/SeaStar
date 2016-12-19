package de.unikoblenz.west.seastar.dataanalysis;

import com.google.common.base.Charsets;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import de.unikoblenz.west.seastar.model.Dataset;
import de.unikoblenz.west.seastar.model.PropertyValue;
import de.unikoblenz.west.seastar.utils.URIUtils;


import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by csarasua.
 */
public class P3Analyzer implements Analyzer {


    Dataset dataset;

    Map<String,Integer> vocabDescMap;
    Map<String,Integer> vocabDescMapPrime;

    // %%%%%
    Map<PropertyValue,Integer> vocabLinksMap;
    Map<PropertyValue,Integer> vocabLinksMapPrime;

    File m31aFile;
    File m31bFile;
    File m31cFile;
    File m32File;


    private static final Logger log = LogManager.getLogger(P3Analyzer.class);


    EntropyCalculator ec;

    String workingDir;
    String workingDirForFileName;



    public P3Analyzer(Dataset dataset, String typelinks)
    {


        this.dataset=dataset;
        ec = new EntropyCalculator();




        workingDir = System.getProperty("user.dir");
        workingDirForFileName = workingDir.replace("\\", "/");


        m31aFile = new File(workingDirForFileName + "/output/m31aFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");
        m31bFile = new File(workingDirForFileName + "/output/m31bFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");
        m31cFile = new File(workingDirForFileName + "/output/m31cFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");


        String ls = System.getProperty("line.separator");

        URIUtils.loadDatasetCatalogue();
        try {
        Files.write("", m31aFile, Charsets.UTF_8);
        //writes header of result file
        Files.append("entity" + "\t" +"typeentity" + "\t"+"vocabdesccardinality" + "\t"+"vocabdescprimecardinality", m31aFile, Charsets.UTF_8);
        Files.append(ls, m31aFile, Charsets.UTF_8);

        Files.write("", m31bFile, Charsets.UTF_8);
        //writes header of result file
        Files.append("entity" + "\t" +"typeentity" + "\t"+"diffratio", m31bFile, Charsets.UTF_8);
        Files.append(ls, m31bFile, Charsets.UTF_8);

        Files.write("", m31cFile, Charsets.UTF_8);
        //writes header of result file
        Files.append("entity" + "\t" +"typeentity" + "\t"+"diffentropy"+ "\t"+"diffNentropy", m31cFile, Charsets.UTF_8);
        Files.append(ls, m31cFile, Charsets.UTF_8);



        /*Files.write("", m32File, Charsets.UTF_8);
        //writes header of result file
        Files.append("entity" + "\t" +"typeentity" + "\t"+"preds", m32File, Charsets.UTF_8);
        Files.append(ls, m32File, Charsets.UTF_8);*/
        } catch (IOException e) {
            e.printStackTrace();
            log.error("error writing files ", e);
        }
    }

    public void analyzeLinks(String entity, String entityType)
    {
       // m31a(entity,entityType);
        m31b(entity, entityType);
        //m31c(entity, entityType);




    }
    private void m31a(String entity, String entityType)
    {
        int vocabsDescMapSize = this.vocabDescMap.size();
        int vocabsDescMapPrimeSize = this.vocabDescMapPrime.size();
        try {

            Files.append(entity+"\t"+entityType+"\t"+vocabsDescMapSize+"\t"+vocabsDescMapPrimeSize+"\t", m31aFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m31aFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in description increase output", e);
        }
    }

    private void m31b(String entity, String entityType)
    {

        MapDifference<String,Integer> difference = Maps.difference(this.vocabDescMap,this.vocabDescMapPrime);
        int countNewDatasets = difference.entriesOnlyOnRight().size();
        int countOriginalDatasets = difference.entriesInCommon().size()+difference.entriesDiffering().size();
        double diffVocabsRatio=0;
        if(countOriginalDatasets!=0)
        {
            diffVocabsRatio = countNewDatasets / countOriginalDatasets;
        }
        else
        {
            diffVocabsRatio = 1.0;
        }


        try {

            Files.append(entity+"\t"+entityType+"\t"+diffVocabsRatio+"\t", m31bFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m31bFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in description increase output", e);
        }
    }


    private void m31c(String entity, String entityType)
    {
        double entropyVocabsDesc = ec.calculateEntropy(vocabDescMap,true);
        double entropyVocabsDescPrime = ec.calculateEntropy(vocabDescMapPrime,true);
        double diffNEntropyVocab = entropyVocabsDescPrime - entropyVocabsDesc;
        entropyVocabsDesc = ec.calculateEntropy(vocabDescMap,false);
        entropyVocabsDescPrime = ec.calculateEntropy(vocabDescMapPrime,false);
        double diffEntropyVocab = entropyVocabsDescPrime - entropyVocabsDesc;

        try {

            Files.append(entity+"\t"+entityType+"\t"+diffEntropyVocab+"\t"+diffNEntropyVocab, m31cFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m31cFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in description increase output", e);
        }

    }


    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public Map<String, Integer> getVocabDescMap() {
        return vocabDescMap;
    }

    public void setVocabDescMap(Map<String, Integer> vocabDescMap) {
        this.vocabDescMap = vocabDescMap;
    }

    public Map<String, Integer> getVocabDescMapPrime() {
        return vocabDescMapPrime;
    }

    public void setVocabDescMapPrime(Map<String, Integer> vocabDescMapPrime) {
        this.vocabDescMapPrime = vocabDescMapPrime;
    }

    public Map<PropertyValue, Integer> getVocabLinksMap() {
        return vocabLinksMap;
    }

    public void setVocabLinksMap(Map<PropertyValue, Integer> vocabLinksMap) {
        this.vocabLinksMap = vocabLinksMap;
    }

    public Map<PropertyValue, Integer> getVocabLinksMapPrime() {
        return vocabLinksMapPrime;
    }

    public void setVocabLinksMapPrime(Map<PropertyValue, Integer> vocabLinksMapPrime) {
        this.vocabLinksMapPrime = vocabLinksMapPrime;
    }

    public File getM31aFile() {
        return m31aFile;
    }

    public void setM31aFile(File m31aFile) {
        this.m31aFile = m31aFile;
    }

    public File getM31bFile() {
        return m31bFile;
    }

    public void setM31bFile(File m31bFile) {
        this.m31bFile = m31bFile;
    }

    public File getM32File() {
        return m32File;
    }

    public void setM32File(File m32File) {
        this.m32File = m32File;
    }

    public static Logger getLog() {
        return log;
    }

    public File getM31cFile() {
        return m31cFile;
    }

    public void setM31cFile(File m31cFile) {
        this.m31cFile = m31cFile;
    }
}
