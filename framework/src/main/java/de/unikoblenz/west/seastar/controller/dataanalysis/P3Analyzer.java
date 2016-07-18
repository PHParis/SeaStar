package de.unikoblenz.west.seastar.controller.dataanalysis;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import de.unikoblenz.west.seastar.model.Dataset;
import de.unikoblenz.west.seastar.model.PropertyValue;
import de.unikoblenz.west.seastar.utils.URIUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogManager;

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
    File m32File;


    private static final Logger log = LoggerFactory.getLogger(P3Analyzer.class);


    EntropyCalculator ec;

    String workingDir;
    String workingDirForFileName;



    public P3Analyzer(Dataset dataset)
    {


        this.dataset=dataset;
        ec = new EntropyCalculator();

        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();
        java.util.logging.Logger.getLogger("global").setLevel(Level.WARNING);

        Marker m = MarkerFactory.getMarker("debug");
        log.isDebugEnabled(m);


        workingDir = System.getProperty("user.dir");
        workingDirForFileName = workingDir.replace("\\", "/");


        m31aFile = new File(workingDirForFileName + "/output/m31aFile_"+ dataset.getTitle()+".tsv");
        m31bFile = new File(workingDirForFileName + "/output/m31bFile_"+ dataset.getTitle()+".tsv");
        m32File = new File(workingDirForFileName + "/output/m32File_"+ dataset.getTitle()+".tsv");

        String ls = System.getProperty("line.separator");

        URIUtils.loadDatasetCatalogue();
        try {
        Files.write("", m31aFile, Charsets.UTF_8);
        //writes header of result file
        Files.append("entity" + "\t" +"typeentity" + "\t"+"vocabdesccardinality" + "\t"+"vocabdescprimecardinality", m31aFile, Charsets.UTF_8);
        Files.append(ls, m31aFile, Charsets.UTF_8);

        Files.write("", m31bFile, Charsets.UTF_8);
        //writes header of result file
        Files.append("entity" + "\t" +"typeentity" + "\t"+"diffentropy", m31bFile, Charsets.UTF_8);
        Files.append(ls, m31bFile, Charsets.UTF_8);

        Files.write("", m32File, Charsets.UTF_8);
        //writes header of result file
        Files.append("entity" + "\t" +"typeentity" + "\t"+"preds", m32File, Charsets.UTF_8);
        Files.append(ls, m32File, Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("error writing files ", e);
        }
    }

    public void analyzeLinks(String entity, String entityType)
    {
        m31a(entity,entityType);
        m31b(entity, entityType);

        m32(entity, entityType);

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
        double entropyVocabsDesc = ec.calculateEntropy(vocabDescMap);
        double entropyVocabsDescPrime = ec.calculateEntropy(vocabDescMapPrime);
        double diffVocab = entropyVocabsDescPrime - entropyVocabsDesc;

        try {

            Files.append(entity+"\t"+entityType+"\t"+diffVocab+"\t", m31bFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m31bFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in description increase output", e);
        }

    }
    private void m32(String entity, String entityType)
    {

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
}
