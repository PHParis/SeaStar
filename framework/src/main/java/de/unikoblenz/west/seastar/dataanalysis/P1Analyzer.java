package de.unikoblenz.west.seastar.dataanalysis;

import com.google.common.base.Charsets;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import de.unikoblenz.west.seastar.model.Dataset;
import de.unikoblenz.west.seastar.model.PropertyValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;


/**
 * Created by csarasua.
 */
public class P1Analyzer implements Analyzer{


    Dataset dataset;

    Map<String,Integer> classificationMap;
    Map<String,Integer> classificationMapPrime;

    Map<PropertyValue,Integer> propertyValueMap;
    Map<PropertyValue,Integer> propertyValueMapPrime;

    Map<String,Integer> propertyMap;
    Map<String,Integer> propertyMapPrime;

    File m11aFile;
    File m11bFile;
    File m11cFile;
    File m12aFile;
    File m12bFile;
    File m12cFile;
    File m13aFile;
    File m13bFile;
    File m13cFile;

    private static final Logger log = LogManager.getLogger(P1Analyzer.class);


    EntropyCalculator ec;

    String workingDir;
    String workingDirForFileName;



    public P1Analyzer(Dataset dataset, String typelinks)
    {
        this.dataset=dataset;
        ec = new EntropyCalculator();




        workingDir = System.getProperty("user.dir");
        workingDirForFileName = workingDir.replace("\\", "/");
        //+ dataset.getTitle()+

        m11aFile = new File(workingDirForFileName + "/output/m11aFile_"+dataset.getTitle()+"_"+typelinks+".tsv");
        m11bFile = new File(workingDirForFileName + "/output/m11bFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");
        m11cFile = new File(workingDirForFileName + "/output/m11cFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");
        m12aFile = new File(workingDirForFileName + "/output/m12aFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");
        m12bFile = new File(workingDirForFileName + "/output/m12bFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");
        m12cFile = new File(workingDirForFileName + "/output/m12cFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");
        m13aFile = new File(workingDirForFileName + "/output/m13aFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");
        m13bFile = new File(workingDirForFileName + "/output/m13bFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");
        m13cFile = new File(workingDirForFileName + "/output/m13cFile_"+ dataset.getTitle()+"_"+typelinks+".tsv");


        String ls = System.getProperty("line.separator");
        try {


            Files.write("", m11aFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" + "typeentity" + "\t" + "classifcardinality" + "\t" + "classifprimecardinality", m11aFile, Charsets.UTF_8);
            Files.append(ls, m11aFile, Charsets.UTF_8);

            Files.write("", m11bFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" + "typeentity" + "\t" + "diffratio", m11bFile, Charsets.UTF_8);
            Files.append(ls, m11bFile, Charsets.UTF_8);

            Files.write("", m11cFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" + "typeentity" + "\t" + "diffentropy"+ "\t" + "diffNentropy", m11cFile, Charsets.UTF_8);
            Files.append(ls, m11cFile, Charsets.UTF_8);


            Files.write("", m12aFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" + "typeentity" + "\t" + "desccardinality" + "\t" + "descprimecardinality", m12aFile, Charsets.UTF_8);
            Files.append(ls, m12aFile, Charsets.UTF_8);

            Files.write("", m12bFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" + "typeentity" + "\t" + "diffratio", m12bFile, Charsets.UTF_8);
            Files.append(ls, m12bFile, Charsets.UTF_8);

            Files.write("", m12cFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" + "typeentity" + "\t" + "diffentropy"+ "\t" + "diffNentropy", m12cFile, Charsets.UTF_8);
            Files.append(ls, m12cFile, Charsets.UTF_8);


            Files.write("", m13aFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" + "typeentity" + "\t" + "desccardinality" + "\t" + "descprimecardinality", m13aFile, Charsets.UTF_8);
            Files.append(ls, m13aFile, Charsets.UTF_8);

            Files.write("", m13bFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" + "typeentity" + "\t" + "diffratio", m13bFile, Charsets.UTF_8);
            Files.append(ls, m13bFile, Charsets.UTF_8);

            Files.write("", m13cFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("entity" + "\t" + "typeentity" + "\t" + "diffentropy"+ "\t" + "diffNentropy", m13cFile, Charsets.UTF_8);
            Files.append(ls, m13cFile, Charsets.UTF_8);


    } catch (IOException e) {
        e.printStackTrace();
        log.error("error writing files ", e);
    }
    }

    public void analyzeLinks(String entity, String entityType)
    {
          //  m11a(entity, entityType);
            m11b(entity, entityType);
           // m11c(entity, entityType);

           // m12a(entity, entityType);
            m12b(entity, entityType);
           // m12c(entity, entityType);

          //  m13a(entity, entityType);
            m13b(entity, entityType);
           // m13c(entity, entityType);

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

        MapDifference<String,Integer> difference = Maps.difference(this.classificationMap,this.classificationMapPrime);
        int countNewClasses = difference.entriesOnlyOnRight().size();
        //the prime inherits also the things on the original one. therefore it will appear in both!
        // multisets: in original we can have Type T1 count 1 and if the target also has T2 the count will be 2
        //entries in common can be entry that is only on the left and inherited to the right too; while in difference there are entries with same key and different count (i.e. with repetitions between source and target)
        int countOriginalClasses = difference.entriesInCommon().size()+difference.entriesDiffering().size();
        double diffClassRatio=0.0;
        if (countOriginalClasses!=0)
        {diffClassRatio = countNewClasses / countOriginalClasses;}
        else
        {diffClassRatio=1;}

        try {

            Files.append(entity+"\t"+entityType+"\t"+diffClassRatio+"\t", m11bFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m11bFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in description increase output", e);
        }
    }
    private void m11c(String entity, String entityType)
    {
        double entropyClassif = ec.calculateEntropy(classificationMap,false);
        double entropyClassifPrime = ec.calculateEntropy(classificationMapPrime,false);
        double diffNEntropyClass = entropyClassifPrime-entropyClassif;
        entropyClassif = ec.calculateEntropy(classificationMap,true);
        entropyClassifPrime = ec.calculateEntropy(classificationMapPrime,true);
        double diffEntropyClass = entropyClassifPrime-entropyClassif;

        try {

            Files.append(entity+"\t"+entityType+"\t"+diffEntropyClass+"\t"+diffNEntropyClass, m11cFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m11cFile, Charsets.UTF_8);
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
    private void m12b(String entity, String entityType )
    {
        MapDifference<PropertyValue,Integer> difference = Maps.difference(this.propertyValueMap,this.propertyValueMapPrime);
        int countNewPVs = difference.entriesOnlyOnRight().size();
        //the prime inherits also the things on the original one. therefore it will appear in both! with equal or different counts
        // prop-value only in common
        int countOriginalPVs = difference.entriesInCommon().size()+difference.entriesDiffering().size();//entriesOnlyOnLeft().size();
        double diffPVRatio=0.0;
        if (countOriginalPVs!=0)
        {diffPVRatio = countNewPVs / countOriginalPVs;}
        else
        {
            diffPVRatio = 1.0;
        }

        try {

            Files.append(entity+"\t"+entityType+"\t"+diffPVRatio+"\t", m12bFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m12bFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in description increase output", e);
        }

    }
    private void m12c(String entity, String entityType)
    {
        double entropyPropV = ec.calculateEntropyP(propertyValueMap,true);
        double entropyPropVPrime = ec.calculateEntropyP(propertyValueMapPrime,true);
        double diffNEntropyPropV = entropyPropVPrime-entropyPropV;
        entropyPropV = ec.calculateEntropyP(propertyValueMap,false);
        entropyPropVPrime = ec.calculateEntropyP(propertyValueMapPrime,false);
        double diffEntropyPropV = entropyPropVPrime-entropyPropV;

        try {

            Files.append(entity+"\t"+entityType+"\t"+diffEntropyPropV+"\t"+diffNEntropyPropV, m12cFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m12cFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in description increase output", e);
        }


    }

    private void m13a(String entity, String entityType )
    {
        int propMapSize = this.propertyMap.size();
        int propMapPrimeSize = this.propertyMapPrime.size();
        try {

            Files.append(entity+"\t"+entityType+"\t"+propMapSize+"\t"+propMapPrimeSize+"\t", m13aFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m13aFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in description increase output", e);
        }

    }
    private void m13b(String entity, String entityType )
    {
        MapDifference<String,Integer> difference = Maps.difference(this.propertyMap,this.propertyMapPrime);
        int countNewPs = difference.entriesOnlyOnRight().size();
        int countOriginalPs = difference.entriesInCommon().size()+difference.entriesDiffering().size();
        double diffPRatio=0.0;
        if(countOriginalPs!=0)
        {
            diffPRatio= countNewPs / countOriginalPs;

        }
        else
        { diffPRatio= 1.0 ;}


        try {

            Files.append(entity+"\t"+entityType+"\t"+diffPRatio+"\t", m13bFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m13bFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("problem when writing in description increase output", e);
        }

    }
    private void m13c(String entity, String entityType)
    {
        double entropyProp = ec.calculateEntropy(propertyMap,true);
        double entropyPropPrime = ec.calculateEntropy(propertyMapPrime,true);
        double diffNEntropyProp = entropyPropPrime-entropyProp;
        entropyProp = ec.calculateEntropy(propertyMap,false);
        entropyPropPrime = ec.calculateEntropy(propertyMapPrime,false);
        double diffEntropyProp = entropyPropPrime-entropyProp;

        try {

            Files.append(entity+"\t"+entityType+"\t"+diffEntropyProp+"\t"+diffNEntropyProp, m13cFile, Charsets.UTF_8);
            String ls = System.getProperty("line.separator");
            Files.append(ls , m13cFile, Charsets.UTF_8);
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

    public File getM11cFile() {
        return m11cFile;
    }

    public void setM11cFile(File m11cFile) {
        this.m11cFile = m11cFile;
    }

    public File getM12cFile() {
        return m12cFile;
    }

    public void setM12cFile(File m12cFile) {
        this.m12cFile = m12cFile;
    }

    public Map<String, Integer> getPropertyMap() {
        return propertyMap;
    }

    public void setPropertyMap(Map<String, Integer> propertyMap) {
        this.propertyMap = propertyMap;
    }

    public Map<String, Integer> getPropertyMapPrime() {
        return propertyMapPrime;
    }

    public void setPropertyMapPrime(Map<String, Integer> propertyMapPrime) {
        this.propertyMapPrime = propertyMapPrime;
    }
}
