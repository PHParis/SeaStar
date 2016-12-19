package de.unikoblenz.west.seastar.model;


import de.unikoblenz.west.seastar.launcher.Main;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.TDBLoader;
import org.apache.jena.tdb.base.file.Location;
import org.apache.jena.tdb.sys.TDBInternal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

        private String uriSpace; // void: patterns for resource URIs e.g. http://dbpedia.org/resource/
        private String vocabulary; //locator
        private String nameSpace; // Given URI for the data set

        private Model model;
        private org.apache.jena.query.Dataset dataset=null;



        String workingDir;
        String workingDirForFileName;

    private static final Logger log= LogManager.getLogger(Dataset.class);



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

                File f = new File(location);
              //  File f = new File(workingDirForFileName+location);
                long length = f.length();
                if(length >= 500000 || f.getAbsolutePath().endsWith("dbpedia-org.nq")) { //5000000



                    // TODO: file paths
                    //String assemblerFile = "file:///C:/Users/csarasua/Documents/test-csarasua-vm/tdb.ttl";

                    InputStream in;
                    if (location.endsWith(".gz")) {
                        in = new GZIPInputStream(new FileInputStream(location));
                    } else {

                        in = new FileInputStream(location);//FileManager.get().open(location);
                    }




                    //-----------------------------------

//Location.create ("target/TDB");
                    Location location = Location.create(workingDirForFileName + "/TDB");

                    // Load some initial data
                    TDBLoader.load(TDBInternal.getBaseDatasetGraphTDB(TDBFactory.createDatasetGraph(location)), in, false);
                    this.dataset = TDBFactory.createDataset(location);
                    //TODO CLEAN THIS PART TDB

                    //dataset.getDefaultModel();

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
                else {

                    if (this.typeOfLocation.equals(TypeOfDatasetLocation.FILEDUMP_NQ)) {
                        this.dataset = RDFDataMgr.loadDataset(location, Lang.NQUADS);

                    } else if (this.typeOfLocation.equals(TypeOfDatasetLocation.FILEDUMP)) {

                        this.model = ModelFactory.createDefaultModel();


                         // if (length < 5000000) {

                            //The data set dump file can be imported to an in-memory model
                            InputStream in;
                            if (location.startsWith("http://")) {
                                model.read(location);
                            } else {

                                // model.read("file:///" + location);

                                if (location.endsWith(".gz")) {
                                    in = new GZIPInputStream(new FileInputStream(location));
                                } else {

                                    in = new FileInputStream(location);
                                }
                                // in = new BufferedInputStream(in);


                                if (location.endsWith(".rdf")) {
                                    model.read(in, null, "RDF/XML");
                                } else if (location.endsWith(".nt")) {
                                    model.read(in, null, "N-Triples");
                                }


                            }




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
               // e.printStackTrace();
                log.error("problem when creating / working a SeaStar Dataset");
                log.error("reason",e);
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


        public org.apache.jena.query.Dataset getDataset() {
        return dataset;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Dataset dataset = (Dataset) o;

        return title.equals(dataset.title);

    }

    @Override
    public int hashCode() {
        return title.hashCode();
    }



}
