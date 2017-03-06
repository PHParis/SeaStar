package de.unikoblenz.west.seastar.dataanalysis;

import de.unikoblenz.west.seastar.model.Dataset;
import de.unikoblenz.west.seastar.model.PropertyValue;
import de.unikoblenz.west.seastar.model.TypeOfDatasetLocation;
import de.unikoblenz.west.seastar.utils.ConfigurationManager;
import de.unikoblenz.west.seastar.utils.Constants;
import de.unikoblenz.west.seastar.utils.URIUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RiotNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Created by csarasua.
 */
public class SeaStarAnalyzer {

    Dataset dataset = null;
    String linksFilePath = null;



    // Set of raw statistics analyzers
    Set<RawStatsAnalyzer> stats;

    // Set of analyzers looking at the links in different dimensions
    List<Analyzer> analyzers;


    private static final Logger log = LogManager.getLogger(SeaStarAnalyzer.class);

    String pickedUpLinksPath;


    String workingDir;
    String workingDirForFileName;


    //TODO:names

    File statsFile;


    File m11aFile;
    File m11bFile;
    Map<String, Integer> classificationMap;
    Map<String, Integer> classificationMapPrime;

    File m12aFile;
    File m12bFile;
    Map<PropertyValue, Integer> propertyValueMap;
    Map<PropertyValue, Integer> propertyValueMapPrime;

    File m21aFile;
    File m21bFile;
    Map<String, Integer> targetMap;
    Map<String, Integer> targetMapPrime;

    File m22aFile;
    File m22bFile;
    Map<String, Integer> datasetMap;
    Map<String, Integer> datasetMapPrime;

    File m23File;
    ///////////////////////////
    Map<String, Integer> propertyMap;
    Map<String, Integer> propertyMapPrime;


    File m31aFile;
    File m31bFile;
    Map<String, Integer> vocabDescMap;
    Map<String, Integer> vocabDescMapPrime;

    File m32File;
    /////////////////////
    Map<String, Integer> vocabLinksMap;
    Map<String, Integer> vocabLinksMapPrime;

    Connection connect;
    PreparedStatement preparedStatement;




    EntityManager em;
    EntityManager emForTargets;

    Map<String,Dataset> targetDatasets=new HashMap<String,Dataset>();


    public SeaStarAnalyzer(Dataset dataset, Set<RawStatsAnalyzer> statsAnalyzer, List<Analyzer> analyzersImpl) {
        this.dataset = dataset;

        this.stats = statsAnalyzer;
        this.analyzers = analyzersImpl;



    }

    public SeaStarAnalyzer(Dataset dataset, String linksFilePath, Set<RawStatsAnalyzer> statsAnalyzer, List<Analyzer> analyzersImpl) {
        this.linksFilePath = linksFilePath;

        this.stats = statsAnalyzer;
        this.analyzers = analyzersImpl;

        em = new EntityManager(dataset);

        this.pickedUpLinksPath = linksFilePath;

        try {
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/" + ConfigurationManager.getInstance().getDBName() + "?"
                            + "user=" + ConfigurationManager.getInstance().getDBUserName() + "&password=" + ConfigurationManager.getInstance().getDBPassword());
        } catch (Exception e) {
            log.error("error connecting database ", e);
        }





        //TODO: seastar.alllinks --> links
        workingDir = System.getProperty("user.dir");
        workingDirForFileName = workingDir.replace("\\", "/");

        URIUtils.loadDatasetCatalogue();


        // Prepare output files
        String ls = System.getProperty("line.separator");


        classificationMap = new HashMap<String, Integer>();
        classificationMapPrime = new HashMap<String, Integer>();

        propertyValueMap = new HashMap<PropertyValue, Integer>();
        propertyValueMapPrime = new HashMap<PropertyValue, Integer>();


        datasetMap = new HashMap<String, Integer>();
        datasetMapPrime = new HashMap<String, Integer>();

        targetMap = new HashMap<String, Integer>();
        targetMapPrime = new HashMap<String, Integer>();

        // %%%%
        propertyMap = new HashMap<String, Integer>();
        propertyMapPrime = new HashMap<String, Integer>();


        vocabDescMap = new HashMap<String, Integer>();
        vocabDescMapPrime = new HashMap<String, Integer>();

        vocabLinksMap = new HashMap<String, Integer>();
        vocabLinksMapPrime = new HashMap<String, Integer>();

    }


    public void processLinks() {
        // Read the links from CSV file
        Reader in = null;
        try {
            in = new FileReader(this.pickedUpLinksPath);
        } catch (FileNotFoundException e) {
            log.error("problem when loading links",e);
        }
        Iterable<CSVRecord> records = null;
        try {

           records = CSVFormat.TDF.parse(in);
            // records = CSVFormat.DEFAULT.withHeader("id","source","predicate","target","datasets","datasett","typelink","datasetspld","datasettpld","datasetcpld").parse(in);
            //records = CSVFormat.DEFAULT.withHeader().parse(in);

        } catch (IOException e) {
            e.printStackTrace();
        }
        // TODO: with database is the same - but update it to db
        String lastEntity = "";
        int numEntities = 0;
        String entityType = null;
        boolean newEntity = false;
        Model sourceModel = ModelFactory.createDefaultModel();
        Model sourceClassifModel = ModelFactory.createDefaultModel();
        Model sourceDescmModel = ModelFactory.createDefaultModel();
        Model sourceConnectModel = ModelFactory.createDefaultModel();


        Model targetModel;
        String source = "";
        Dataset targetDataset=null;


        try {
            for (CSVRecord record : records) {

                source = record.get(1);
                //source = record.get("source");
                if (source.startsWith("mailto:")) {
                    continue;
                }
                if (!source.equals(lastEntity)) //new!
                {

                    sourceModel = ModelFactory.createDefaultModel();
                    targetModel = ModelFactory.createDefaultModel();
                    newEntity = true;
                } else {
                    newEntity = false;
                }

                String predicate = record.get(2);
                String target = record.get(3);
               // String predicate = record.get("predicate");
                //String target = record.get("target");


                String datasets = record.get(4);

                String datasetspld = record.get(7);
                String datasettpld = record.get(8);


                if (newEntity)

                {

                    if (numEntities != 0)// the links of the entity were processed, call the analyzers
                    {


                        analyze(lastEntity, entityType);


                    }


                    numEntities++;


                    // For each entity with all its links
                    classificationMap = new HashMap<String, Integer>();
                    classificationMapPrime = new HashMap<String, Integer>();

                    propertyValueMap = new HashMap<PropertyValue, Integer>();
                    propertyValueMapPrime = new HashMap<PropertyValue, Integer>();


                    datasetMap = new HashMap<String, Integer>();
                    datasetMapPrime = new HashMap<String, Integer>();

                    targetMap = new HashMap<String, Integer>();
                    targetMapPrime = new HashMap<String, Integer>();

                    // %%%%
                    propertyMap = new HashMap<String, Integer>();
                    propertyMapPrime = new HashMap<String, Integer>();


                    vocabDescMap = new HashMap<String, Integer>();
                    vocabDescMapPrime = new HashMap<String, Integer>();

                    vocabLinksMap = new HashMap<String, Integer>();
                    vocabLinksMapPrime = new HashMap<String, Integer>();

                    sourceModel = em.getEntityModelFromDataset(source);
                    Model originalSourceModel = em.getOriginalModel(sourceModel);

                    em.getAllModels(originalSourceModel, sourceClassifModel, sourceDescmModel, sourceConnectModel);

                    String base = sourceModel.getNsPrefixURI("base");


                    // Process Maps of source
                    StmtIterator sourceStIt = originalSourceModel.listStatements();
                    while (sourceStIt.hasNext()) {
                        Statement st = sourceStIt.nextStatement();

                        if (st.getPredicate().getURI().equals(Constants.NS_RDF + "type") && st.getObject().isResource()) {
                            // add to the classification
                            String type = st.getObject().asResource().getURI();
                            Integer count = 1;
                            entityType = type;
                            if (classificationMap.containsKey(type)) {
                                count = classificationMap.get(type);
                                count++;

                            }

                            classificationMap.put(type, count);
                        } else {
                            // add to the PV
                            String pred = st.getPredicate().getURI();
                            String obj = null;
                            if (st.getObject().isURIResource() && st.getObject().asResource().getURI().startsWith("http://")) {
                                //!st.getObject().asResource().getURI().startsWith("mailto")&& !st.getObject().asResource().getURI().startsWith("urn:")

                                obj = st.getObject().asResource().getURI();

                                String dataset = URIUtils.pld(obj);
                                Integer count = 1;
                                if (datasetMap.containsKey(dataset)) {
                                    count = datasetMap.get(dataset);
                                    count++;
                                }
                                datasetMap.put(dataset, count);


                                Integer countT = 1;
                                if (targetMap.containsKey(obj)) {
                                    countT = targetMap.get(obj);
                                    countT++;
                                }
                                targetMap.put(obj, countT);
                            } else if (st.getObject().isLiteral()) {
                                obj = st.getObject().asLiteral().toString();
                            }


                            if (obj != null) {
                                PropertyValue pv = new PropertyValue(pred, obj);
                                Integer count = 1;
                                if (propertyValueMap.containsKey(pv)) {
                                    count = propertyValueMap.get(pv);
                                    count++;

                                }
                                propertyValueMap.put(pv, count);

                            }


                            String prop = st.getPredicate().getURI();
                            Integer count = 1;
                            if (propertyMap.containsKey(prop)) {
                                count = propertyMap.get(prop);
                                count++;
                            }
                            propertyMap.put(prop, count);


                            // add to the data set (if multiple links to same entity still several times the data set)

                        }
                    }


                    classificationMapPrime.putAll(classificationMap);
                    propertyValueMapPrime.putAll(propertyValueMap);

                    propertyMapPrime.putAll(propertyMap);

                    targetMapPrime.putAll(targetMap);
                    datasetMapPrime.putAll(datasetMap);


                }


                if (predicate.equals(Constants.NS_OWL + "sameAs") || (predicate.equals(Constants.NS_SKOS + "exactMatch"))) {
                    // add classif, prop, propvalue, data set of object

                    if (targetDatasets.size()==0)
                    {
                        //initialize
                        String sourcePLD= URIUtils.pld(source);
                        String getTargetDatasets="Select distinct datasettpld from alllinks where typelink=? and datasetspld=?";

                        preparedStatement=connect.prepareStatement(getTargetDatasets);
                        preparedStatement.setString(1,"i");
                        preparedStatement.setString(2,sourcePLD);
                        ResultSet results = preparedStatement.executeQuery();
                       int i=0;
                        while(results.next())
                        {


                            String targetiPLD = results.getString("datasettpld");
                            log.info("looking for the target data set: "+targetiPLD);

                            //TODO: move to URIutils
                            String nameFileT = targetiPLD.replace("/", "");
                            String nameFileTT = nameFileT.replace("http:","");
                            String nameFile = nameFileTT.replace(".","-");
                            try{

                                targetDataset = new Dataset("target"+i, TypeOfDatasetLocation.FILEDUMP_NQ, ConfigurationManager.getInstance().getDataDirectoryPath() + "/datasets_nq/dataset_" + nameFile + ".nq", null, null, null); //looking for its dataset
                                targetDatasets.put(nameFile,targetDataset);

                            }catch(RiotNotFoundException e)
                            {
                                log.error("there was no file with title"+nameFile);
                            }
                        }


                    }
                    if (!target.startsWith("mailto")) {


                        String targetPLD=URIUtils.pld(target);
                        String nameFileT = targetPLD.replace("/", "");
                        String nameFileTT = nameFileT.replace("http:","");
                        String nameFile = nameFileTT.replace(".","-");

                        boolean skip=true;
                        targetDataset=null;

                        if (targetDatasets.containsKey(nameFile))
                        {
                            targetDataset = targetDatasets.get(nameFile);
                            skip=false;

                        }

                        emForTargets=new EntityManager(targetDataset);
                        targetModel=ModelFactory.createDefaultModel();
                        try {
                            if (!skip) {
                                targetModel = emForTargets.getEntityModelFromDataset(target);
                            }
                        }
                        catch(RiotNotFoundException e)
                        {
                            log.error("there was no file with title"+nameFile+" for target URI "+target);


                        }

                        //targetModel = em.getEntityModelDeref(target, source); // the target will not be in the data set!


                        Model originalTargetModel = em.getOriginalModel(targetModel);
                        Model targetClassModel = ModelFactory.createDefaultModel();
                        Model targetDescmModel = ModelFactory.createDefaultModel();
                        Model targetConnModel = ModelFactory.createDefaultModel();

                        em.getAllModels(originalTargetModel, targetClassModel, targetDescmModel, targetConnModel);

                        // only classifc and descm

                        StmtIterator targetCStIt = targetClassModel.listStatements();

                        while (targetCStIt.hasNext()) {
                            Statement st = targetCStIt.nextStatement();

                            String type = st.getObject().asResource().getURI();

                            Integer count = 1;
                            entityType = type;
                            if (classificationMapPrime.containsKey(type)) {
                                count = classificationMapPrime.get(type);
                                count++;

                            }

                            classificationMapPrime.put(type, count);

                        }

                        StmtIterator targetDescStIt = targetDescmModel.listStatements();


                        while (targetDescStIt.hasNext()) {
                            Statement st = targetDescStIt.nextStatement();
                            String pred = st.getPredicate().getURI();
                            String obj = "blank";
                            if (st.getObject().isURIResource()) {

                                obj = st.getObject().asResource().getURI();
                                //could also inherit like Desc

                            } else if (st.getObject().isLiteral()) {
                                obj = st.getObject().asLiteral().toString();
                            }
                            // blank nodes will not be repeated because they are not identifiable and recognizable URIs

                            PropertyValue pv = new PropertyValue(pred, obj);
                            Integer count = 1;

                            // System.out.println("target:"+target);

                            if (propertyValueMapPrime.containsKey(pv.hashCode())) {
                                count = propertyValueMapPrime.get(pv.hashCode());
                                count++;

                            }
                            propertyValueMapPrime.put(pv, count);


                        }

                        StmtIterator targetConntIt = targetConnModel.listStatements();

                        while (targetConntIt.hasNext()) {
                            Statement st = targetConntIt.nextStatement();
                            Integer count = 1;
                            String obj = st.getObject().asResource().getURI();
                            if (targetMapPrime.containsKey(obj)) {
                                count = targetMapPrime.get(obj);
                                count++;

                            }

                            targetMapPrime.put(obj, count);

                        }

                    }



                } else {
                    // description needs to take the propvalue


                    PropertyValue pv = new PropertyValue(predicate, target);
                    Integer count = 1;
                    if (propertyValueMapPrime.containsKey(pv)) {
                        count = propertyValueMapPrime.get(pv);
                        count++;

                    }
                    propertyValueMapPrime.put(pv, count);
                }

                // in any case the target, the dataset and the property maps register the link

                Integer count = 1;
                if (targetMapPrime.containsKey(target)) {
                    count = targetMapPrime.get(target);
                    count++;
                }
                targetMapPrime.put(target, count);

                if (!target.startsWith("mailto")) {

                    String dataset = URIUtils.pld(target);
                    count = 1;
                    if (datasetMapPrime.containsKey(dataset)) {
                        count = datasetMapPrime.get(dataset);
                        count++;
                    }
                    datasetMapPrime.put(dataset, count);

                }
/*
                PropertyValue pv = new PropertyValue(predicate,target);
                count=1;
                if (propertyValueMapPrime.containsKey(pv.hashCode()))
                {
                    count=propertyValueMapPrime.get(pv.hashCode());
                    count++;

                }
                propertyValueMapPrime.put(pv,count);
                */


                count = 1;
                if (propertyMapPrime.containsKey(predicate)) {
                    count = propertyMapPrime.get(predicate);
                    count++;
                }
                propertyMapPrime.put(predicate, count);


                lastEntity = new String(source);


            }
            analyze(lastEntity, entityType);
        }
        catch(Exception e)
        {
            log.error("problem when processing links",e);
        }



    }

    public void analyze(String entityToAnalyze, String entityType) {
        for (String key : propertyMap.keySet()) {
            String vocab = URIUtils.vocabulary(key);

            Integer count = 1;
            if (vocabDescMap.containsKey(vocab)) {
                count = vocabDescMap.get(vocab);
                count++;
            }
            vocabDescMap.put(vocab, count);


        }

        for (String key : propertyMapPrime.keySet()) {
            String vocab = URIUtils.vocabulary(key);

            Integer count = 1;
            if (vocabDescMapPrime.containsKey(vocab)) {
                count = vocabDescMapPrime.get(vocab);
                count++;
            }
            vocabDescMapPrime.put(vocab, count);


        }


        for (Analyzer analyz : this.analyzers) {

            if (analyz instanceof P1Analyzer) {
                ((P1Analyzer) analyz).setClassificationMap(classificationMap);
                ((P1Analyzer) analyz).setClassificationMapPrime(classificationMapPrime);

                ((P1Analyzer) analyz).setPropertyValueMap(propertyValueMap);
                ((P1Analyzer) analyz).setPropertyValueMapPrime(propertyValueMapPrime);

                ((P1Analyzer) analyz).setPropertyMap(propertyMap);
                ((P1Analyzer) analyz).setPropertyMapPrime(propertyMapPrime);
            } else if (analyz instanceof P2Analyzer) {
                ((P2Analyzer) analyz).setTargetMap(targetMap);
                ((P2Analyzer) analyz).setTargetMapPrime(targetMapPrime);

                ((P2Analyzer) analyz).setDatasetMap(datasetMap);
                ((P2Analyzer) analyz).setDatasetMapPrime(datasetMapPrime);

            } else if (analyz instanceof P3Analyzer) {
                ((P3Analyzer) analyz).setVocabDescMap(vocabDescMap);
                ((P3Analyzer) analyz).setVocabDescMapPrime(vocabDescMapPrime);
            }

            analyz.analyzeLinks(entityToAnalyze, entityType);
        }
    }


}
