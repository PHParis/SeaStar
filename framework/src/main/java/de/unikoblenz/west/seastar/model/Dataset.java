package de.unikoblenz.west.seastar.model;


import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.TDBLoader;
import org.apache.jena.tdb.base.file.Location;
import org.apache.jena.tdb.sys.TDBInternal;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/*
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.tdb.TDBFactory;*/
//import org.apache.jena.arq.*;
//import org.apache.jena.arq.*;
//import org.apache.jena.rdf.model.Model;
//import org.apache.jena.rdf.model.ModelFactory;
//import org.apache.jena.tdb.*;

/**
 * Created by csarasua.
 */
public class Dataset {






        private String title; // as for searching
        private TypeOfDatasetLocation typeOfLocation;
        private String location; // can be the Path of a file or a SPARQL endpoint -
        // distinguished by file:/// if local otherwise
        // add tyoe of location

        private String uriSpace;
        private String vocabulary; //locator
        private String nameSpace;

        private Model model;


        String workingDir;
        String workingDirForFileName;


        public Dataset(String title, TypeOfDatasetLocation typeLoc,
                       String loc, String uriSpa, String vocab, String ns) {
            try {

                this.title = title;
                this.typeOfLocation = typeLoc;
                this.location = loc;
                this.uriSpace = uriSpa;
                this.vocabulary = vocab;
                this.nameSpace = ns;


                workingDir = System.getProperty("user.dir");
             //    workingDir = System.getProperty("user.home");
                workingDirForFileName = workingDir.replace("\\", "/");



                Model vocabularyModel = ModelFactory.createDefaultModel();
                // Load the input data set into a model(file-based or DB-backend) only if it is a File, otherwise the queries are executed against the SPARQL Endpoint

                if (this.typeOfLocation.equals(TypeOfDatasetLocation.FILEDUMP)) {

                    this.model = ModelFactory.createDefaultModel();

                    File f = new File(workingDirForFileName+location);
                    long length = f.length();

                    if (length < 5000000) { // if (length < 5000000) {

                        //The data set dump file can be imported to an in-memory model

                        if (location.startsWith("http://")) {
                            model.read(location);
                        } else {

                            // model.read("file:///" + location);
                            InputStream in;
                            if (location.endsWith(".gz")) {
                                in = new GZIPInputStream(new FileInputStream(location));
                            } else {

                                in = new FileInputStream(location);
                            }
                            // in = new BufferedInputStream(in);


                            model.read(in, null, "RDF/XML");

                        }

                    } else {



                        // TODO: file paths
                        //String assemblerFile = "file:///C:/Users/csarasua/Documents/test-csarasua-vm/tdb.ttl";

                        InputStream in;
                        if (location.endsWith(".gz")) {
                            in = new GZIPInputStream(new FileInputStream(location));
                        } else {

                            in = new FileInputStream(workingDirForFileName+location);//FileManager.get().open(location);
                        }




                        //-----------------------------------

//Location.create ("target/TDB");
                        Location location = Location.create(workingDirForFileName + "/TDB");

                        // Load some initial data
                        TDBLoader.load(TDBInternal.getBaseDatasetGraphTDB(TDBFactory.createDatasetGraph(location)), in, false);
                        org.apache.jena.query.Dataset dataset = TDBFactory.createDataset(location);


                    /*
                    //test querying with SPARQL
                    dataset.begin(ReadWrite.READ);
                        //http://www.okkam.org/oaie/person1-Person730

                   // String queryString = "DESCRIBE <http://cbasewrap.ontologycentral.com/person/andreas-harth#id>";
                      //  String queryString = "DESCRIBE <http://data.dcs.shef.ac.uk/group/oak>";
                        String queryString="DESCRIBE <http://bio2rdf.org/refseq:332847203>";

                    Query query = QueryFactory.create(queryString);
                    QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
                    try {
                        Model results = qexec.execDescribe();
                        StmtIterator stIt = results.listStatements();
                        while (stIt.hasNext()) {
                            Statement st = stIt.nextStatement();
                            System.out.println(st.getSubject().getLocalName().toString() + " " + st.getPredicate().getLocalName().toString() + " " + st.getObject().toString() + " ");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        dataset.abort();
                    } finally {
                        qexec.close();

                        dataset.end();

                    }*/


                    }

              }

                //load the vocabulary -- should be done like before if too big store it in Db


                if (vocabulary != null) {
                    if (vocabulary.startsWith("http://")) {
                        vocabularyModel.read(vocabulary);
                    } else {

                        // model.read("file:///" + location);

                        InputStream in = new FileInputStream(vocabulary);
                        in = new BufferedInputStream(in);


                        vocabularyModel.read(in, null);

                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        public String getTitle() {
            return title;
        }


        public void setTitle(String title) {
            this.title = title;
        }


        public TypeOfDatasetLocation getTypeOfLocation() {
            return typeOfLocation;
        }


        public void setTypeOfLocation(TypeOfDatasetLocation typeOfLocation) {
            this.typeOfLocation = typeOfLocation;
        }


        public String getLocation() {
            return location;
        }


        public void setLocation(String location) {
            this.location = location;
        }


        public String getUriSpace() {
            return uriSpace;
        }


        public void setUriSpace(String uriSpace) {
            this.uriSpace = uriSpace;
        }


        public String getVocabulary() {
            return vocabulary;
        }


        public void setVocabulary(String vocabulary) {
            this.vocabulary = vocabulary;
        }


        public String getNameSpace() {
            return nameSpace;
        }


        public void setNameSpace(String nameSpace) {
            this.nameSpace = nameSpace;
        }


        public Model getModel() {
            return model;
        }


        public void setModel(Model model) {
            this.model = model;
        }





}
