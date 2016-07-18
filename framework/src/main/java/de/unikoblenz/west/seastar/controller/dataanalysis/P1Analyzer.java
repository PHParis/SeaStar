package de.unikoblenz.west.seastar.controller.dataanalysis;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import de.unikoblenz.west.seastar.model.Dataset;
import de.unikoblenz.west.seastar.model.PropertyValue;
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
public class P1Analyzer implements Analyzer{


    Dataset dataset;

    Map<String,Integer> classificationMap;
    Map<String,Integer> classificationMapPrime;

    Map<PropertyValue,Integer> propertyValueMap;
    Map<PropertyValue,Integer> propertyValueMapPrime;

    File m11aFile;
    File m11bFile;
    File m12aFile;
    File m12bFile;

    private static final Logger log = LoggerFactory.getLogger(P1Analyzer.class);


    EntropyCalculator ec;

    String workingDir;
    String workingDirForFileName;



    public P1Analyzer(Dataset dataset)
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

        m11aFile = new File(workingDirForFileName + "/output/m11aFile_"+ dataset.getTitle()+".tsv");
        m11bFile = new File(workingDirForFileName + "/output/m11bFile_"+ dataset.getTitle()+".tsv");
        m12aFile = new File(workingDirForFileName + "/output/m12aFile_"+ dataset.getTitle()+".tsv");
        m12bFile = new File(workingDirForFileName + "/output/m12bFile_"+ dataset.getTitle()+".tsv");


        String ls = System.getProperty("line.separator");
        try {

            Files.write("", m11aFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" + "typeentity" + "\t" + "classifcardinality" + "\t" + "classifprimecardinality", m11aFile, Charsets.UTF_8);
            Files.append(ls, m11aFile, Charsets.UTF_8);

            Files.write("", m11bFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" + "typeentity" + "\t" + "diffentropy", m11bFile, Charsets.UTF_8);
            Files.append(ls, m11bFile, Charsets.UTF_8);

            Files.write("", m12aFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" + "typeentity" + "\t" + "desccardinality" + "\t" + "descprimecardinality", m12aFile, Charsets.UTF_8);
            Files.append(ls, m12aFile, Charsets.UTF_8);

            Files.write("", m12bFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" + "typeentity" + "\t" + "diffentropy", m12bFile, Charsets.UTF_8);
            Files.append(ls, m12bFile, Charsets.UTF_8);
    } catch (IOException e) {
        e.printStackTrace();
        log.error("error writing files ", e);
    }
    }

    public void analyzeLinks(String entity, String entityType)
    {
            m11a(entity, entityType);
            m11b(entity, entityType);

            m12a(entity, entityType);
            m12b(entity, entityType);

    }
    private void m11a(String entity, String entityType)
    {
       int classMapSize = this.classificationMap.size();
       int classMapPrimeSize = this.classificationMapPrime.size();

        try {

            Files.append(entity+"\t"+entityType+"\t"+classMapSize+"\t"+classMapPrimeSize+"\t", m11aFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m11aFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in description increase output", e);
        }
    }
    private void m11b(String entity, String entityType)
    {
        double entropyClassif = ec.calculateEntropy(classificationMap);
        double entropyClassifPrime = ec.calculateEntropy(classificationMapPrime);
        double diffClass = entropyClassifPrime-entropyClassif;

        try {

            Files.append(entity+"\t"+entityType+"\t"+diffClass+"\t", m11bFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m11bFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in description increase output", e);
        }
    }
    private void m12a(String entity, String entityType )
    {
        int propvMapSize = this.propertyValueMap.size();
        int propvMapPrimeSize = this.propertyValueMapPrime.size();
        try {

            Files.append(entity+"\t"+entityType+"\t"+propvMapSize+"\t"+propvMapPrimeSize+"\t", m12aFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m12aFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in description increase output", e);
        }

    }
    private void m12b(String entity, String entityType)
    {
        double entropyPropV = ec.calculateEntropyP(propertyValueMap);
        double entropyPropVPrime = ec.calculateEntropyP(propertyValueMapPrime);
        double diffPropV = entropyPropVPrime-entropyPropV;

        try {

            Files.append(entity+"\t"+entityType+"\t"+diffPropV+"\t", m12bFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m12bFile, Charsets.UTF_8);
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

    public Map<String, Integer> getClassificationMap() {
        return classificationMap;
    }

    public void setClassificationMap(Map<String, Integer> classificationMap) {
        this.classificationMap = classificationMap;
    }

    public Map<String, Integer> getClassificationMapPrime() {
        return classificationMapPrime;
    }

    public void setClassificationMapPrime(Map<String, Integer> classificationMapPrime) {
        this.classificationMapPrime = classificationMapPrime;
    }

    public Map<PropertyValue, Integer> getPropertyValueMap() {
        return propertyValueMap;
    }

    public void setPropertyValueMap(Map<PropertyValue, Integer> propertyValueMap) {
        this.propertyValueMap = propertyValueMap;
    }

    public Map<PropertyValue, Integer> getPropertyValueMapPrime() {
        return propertyValueMapPrime;
    }

    public void setPropertyValueMapPrime(Map<PropertyValue, Integer> propertyValueMapPrime) {
        this.propertyValueMapPrime = propertyValueMapPrime;
    }

    public File getM11aFile() {
        return m11aFile;
    }

    public void setM11aFile(File m11aFile) {
        this.m11aFile = m11aFile;
    }

    public File getM11bFile() {
        return m11bFile;
    }

    public void setM11bFile(File m11bFile) {
        this.m11bFile = m11bFile;
    }

    public File getM12aFile() {
        return m12aFile;
    }

    public void setM12aFile(File m12aFile) {
        this.m12aFile = m12aFile;
    }

    public File getM12bFile() {
        return m12bFile;
    }

    public void setM12bFile(File m12bFile) {
        this.m12bFile = m12bFile;
    }
}
