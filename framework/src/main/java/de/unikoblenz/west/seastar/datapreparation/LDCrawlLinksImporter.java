package de.unikoblenz.west.seastar.datapreparation;

import de.unikoblenz.west.seastar.utils.ConfigurationManager;
import de.unikoblenz.west.seastar.utils.Constants;
import de.unikoblenz.west.seastar.utils.URIUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.sql.*;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import java.util.zip.ZipFile;

/**
 * Created by csarasua.
 * <p>
 * Processes the set of links of the Mannheim crawl and shows some basic statistics, to understand its content.
 * (Helper class).
 */
public class LDCrawlLinksImporter implements LinkImporter{


    private static final Logger log = LogManager.getLogger(LDCrawlLinksImporter.class);

    long counter=0;

    String linksFilePath;
    String catalogueFilePath;

    PreparedStatement statement = null;

    Connection connect;

    Map<String,String> datasets = new HashMap<String,String>();
    // Database connection management


    /**
     * @param pathLinks     path of input file containing the links to be processed
     * @param pathCatalogue path of input file containing the information about the data catalogue (name of data sets and pay level domains)
     *
     */
    public LDCrawlLinksImporter(String pathLinks, String pathCatalogue) {


        this.linksFilePath = pathLinks;
        this.catalogueFilePath = pathCatalogue;



        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");
            // Setup the connection with the DB
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/" + ConfigurationManager.getInstance().getDBName() + "?"
                            + "user=" + ConfigurationManager.getInstance().getDBUserName() + "&password=" +ConfigurationManager.getInstance().getDBPassword() + "");
        }
        catch(Exception e)
        {log.error("opening connection ",e);}

            //reads allPredicates
        //readPredicates(pathLinks);


    }
    public LDCrawlLinksImporter ()
    {



        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");
            // Setup the connection with the DB
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/" + ConfigurationManager.getInstance().getDBName() + "?"
                            + "user=" + ConfigurationManager.getInstance().getDBUserName() + "&password=" + ConfigurationManager.getInstance().getDBPassword() + "");
        }
        catch(Exception e)
        {log.error("opening connection ",e);}
    }
public void closeConnections()
{
    try {
        connect.close();
    } catch (SQLException e) {
        log.error("error closing connection ", e);
    }
}

    public void loadDatasetCatalogue() {

        URIUtils.loadDatasetCatalogue();
        /*
        Reader in = null;
        try {
            in = new FileReader(this.catalogueFilePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Iterable<CSVRecord> records = null;
        try {
            records = CSVFormat.TDF.withHeader("dataset", "category").parse(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (CSVRecord record : records) {



            //log.isDebugEnabled(m);
            //log.info("logging on");

            String datasetPLD = record.get("dataset");
           // log.info("PLD of data set: " + datasetPLD);

            String category = record.get("category");
           // log.info("LOD topical domain of data set: " + category);

            this.datasets.put(datasetPLD, category);
            insertDataset(datasetPLD, category);

        }
        */

    }

    private void insertDataset(String pld, String category) {
        Connection connect = null;
        Statement statement = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;


        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");
            // Setup the connection with the DB
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/"+ConfigurationManager.getInstance().getDBName()+"?"
                            + "user="+ConfigurationManager.getInstance().getDBUserName()+"&password="+ ConfigurationManager.getInstance().getDBPassword());

            log.info("connected to database");

            statement = connect.createStatement();
            log.info("statement created");

            // find the type of link it is
            if (!pld.equals("dataset") && !category.equals("category")) {
                String sql = "INSERT INTO datasets(pld,category) VALUES('" + pld + "','" + category + "')";
                statement.executeUpdate(sql);
            }


        } catch (Exception e) {
            log.info(e.getMessage().toString());
        } finally {
            try {
                if (statement != null)
                    connect.close();
            } catch (SQLException se) {
            }// do nothing
            try {
                if (connect != null)
                    connect.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }//end finally try
        }

    }

  /*  public void addDatasetsToLinks() {

        Statement statement = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {


            // log.info("connected to database");

            statement = connect.createStatement();
            // log.info("statement created");

            //for each pld in datasets


          String sqlLinks = "SELECT source,predicate,target, datasets, datatasett, typelink FROM seastar.links";
            ResultSet rsLinks = statement.executeQuery(sqlLinks);
            while(rsLinks.next())
            {
                String source = rsLinks.getString("source");
                String predicate = rsLinks.getString("predicate");
                String target = rsLinks.getString("target");

                String datasets = rsLinks.getString("datasets");
                String datasett = rsLinks.getString("datasett");
                String typelinks = rsLinks.getString("typelink");

                URIUtils

                String sqlPLD = "SELECT pld FROM datasets WHERE "
            }


            // find the type of link it is
            String sql = "SELECT id,pld FROM datasets";
            ResultSet rs = statement.executeQuery(sql);
            int id = 0;
            String pld = "unknown";
            while (rs.next()) {
                id = rs.getInt("id");
                pld = rs.getString("pld");

                sql = "SELECT id FROM links WHERE source LIKE '" + pld + "%'";
                //  statement = connect.createStatement();
                ResultSet rsLinks = statement.executeQuery(sql);
                while (rsLinks.next()) {
                    int idLink = rsLinks.getInt("id");
                    sql = "UPDATE links SET datasetspld=" + pld + " WHERE id=" + idLink + "";
                    statement.executeUpdate(sql);
                }

                sql = "SELECT id FROM links WHERE target LIKE '" + pld + "%'";

                rsLinks = statement.executeQuery(sql);
                while (rsLinks.next()) {
                    int idLink = rsLinks.getInt("id");
                    sql = "UPDATE links SET datasettpld=" + pld + " WHERE id=" + idLink + "";

                    statement.executeUpdate(sql);
                }
            }


        } catch (Exception e) {
            log.info("problem adding the data sets info" + e.getMessage().toString() + e.getStackTrace());
        }


    }*/

    private void insertLink(String s, String p, String o, String c,  Connection connect) {



        try {


            //log.info("connected to database");




            //   log.info("statement created");

            // find the type of link it is
            //typ: 't' term class or property defined, 'c' classification of entitiy, 'm' mapping class mapping ontology term, 'i' identity, 'r' relationship, 'o' other (seeAlso), 'd' dataset (void)
            String type = "r";

            if(p.equals(Constants.NS_RDF+"type") && (o.equals(Constants.NS_OWL+"Class") || o.equals(Constants.NS_RDFS+"Class") || o.equals(Constants.NS_SKOS+"Concept")
                    || o.equals(Constants.NS_RDFS+"Property") || o.equals(Constants.NS_OWL+"DatatypeProperty") || o.equals(Constants.NS_OWL+"ObjectProperty") || o.equals(Constants.NS_OWL+"AnnotationProperty")
            || o.equals(Constants.NS_SKOS+"ConceptScheme") || o.equals(Constants.NS_OWL+"AsymmetricProperty") || o.equals(Constants.NS_OWL+"TransitiveProperty") || o.equals(Constants.NS_OWL+"SymmetricProperty")
                    || o.equals(Constants.NS_OWL+"NamedIndividual")))
            {
                type = "t";

            }
            else if(p.equals(Constants.NS_RDF+"type") && (!o.equals(Constants.NS_OWL+"Class") && !o.equals(Constants.NS_RDFS+"Class") && !o.equals(Constants.NS_SKOS+"Concept")
                    && !o.equals(Constants.NS_RDFS+"Property") && !o.equals(Constants.NS_OWL+"DatatypeProperty") && !o.equals(Constants.NS_OWL+"ObjectProperty")))
            {
                type = "c";
            }
            else if(p.equals(Constants.NS_OWL+"equivalentClass")||(p.equals(Constants.NS_OWL+"equivalentProperty")) || (p.equals(Constants.NS_RDFS+"subClassOf")) ||
                    (p.equals(Constants.NS_SKOS+"mappingRelation")) || (p.equals(Constants.NS_SKOS+"narrowMatch")) || (p.equals(Constants.NS_SKOS+"broadMatch"))
                    || (p.equals(Constants.NS_SKOS+"closeMatch")))
            {
                type ="m";
            }
            else if( p.equals(Constants.NS_OWL+"sameAs")|| p.equals(Constants.NS_SKOS+"exactMatch"))
            {
                type="i";
            }
            else if( p.equals(Constants.NS_RDFS+"seeAlso")|| (p.equals(Constants.NS_SKOS+"relatedMatch")) )
            {
                type="o";
            }
            else if(p.startsWith(Constants.NS_VOID) || (p.equals(Constants.NS_RDF+"type") && (o.equals("http://www.w3.org/ns/sparql-service-description#Service") || o.equals("http://www.w3.org/ns/sparql-service-description#Graph") || o.equals("http://rdfs.org/ns/void#Dataset") || o.equals("http://www.w3.org/ns/dcat#Dataset") || o.equals("http://www.w3.org/ns/dcat#Download") )  ) )
            {
                type="d";
            }
            else if(p.equals(Constants.NS_RDF+"type") && (o.equals(Constants.NS_OWL+"Ontology")))
            {
                type="v";
            }
            // I also had "similar s"


            //String sql = "INSERT INTO links(source,predicate,target, datasets,typelink) VALUES ('" + s + "','" + p + "','" + o + "','"+ c + "','" + type + "')";
            statement.setString(1, s);
            statement.setString(2, p);
            statement.setString(3, o);
            statement.setString(4, c);
            statement.setString(5, type);
            String datasetspld = URIUtils.pld(s);
            String datasettpld = URIUtils.pld(o);
            String datasetcpld = URIUtils.pld(c);
            statement.setString(6, datasetspld);
            statement.setString(7, datasettpld);
            statement.setString(8, datasetcpld);

            statement.addBatch();
            counter++;

            if (counter % 1000 == 0) {
                statement.executeBatch();
                connect.commit();

                statement.clearBatch();
                statement.clearParameters();
            }
            //statement.executeUpdate();

            //log.info(sql);



        } catch (Exception e) {

            log.info("problem inserting links " + e.getMessage().toString() + e.getStackTrace());
            log.info("subject was "+s+" predicate was "+p+" object was "+o);
            try {
                connect.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }



    }


    private void insertPredicate(String predicate, Connection connect) {

        Statement statement = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {


            statement = connect.createStatement();

            //checks if the predicate exists
            String sql = "SELECT id FROM predicates WHERE uri='" + predicate + "'";
            ResultSet rs = statement.executeQuery(sql);
            int id = 0;
            while (rs.next()) {
                id = rs.getInt("id");

            }
            if (id == 0) {
                //insert only when it does not exist
                URI uriPredicate = new URI(predicate);
                sql = "INSERT INTO predicates(uri) VALUE ('" + uriPredicate.toString() + "')";
                statement.executeUpdate(sql);

            }


        } catch (Exception e) {
            log.info(e.getMessage().toString());
        } finally {
            try {
                resultSet.close();
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

   /* private void addPredsPerDataset(String predicate, String dataset, Connection connect) {


        PreparedStatement statement = null;
        ResultSet rs = null;


        try {




            //checks if the predicate exists
            //String sql = "SELECT id,count FROM linksperdataset WHERE uripred='" + predicate + "' AND uripld='" + dataset + "'";
            String sql = "SELECT id,count FROM linksperdataset WHERE uripred=? AND uripld=?";
            statement = connect.prepareStatement(sql);
            statement.setString(1, predicate);
            statement.setString(2, dataset);
            rs = statement.executeQuery();
            int count = 0;
            int id = 0;
            if (rs.next()) {
                count = rs.getInt("count");
                id = rs.getInt("id");

            }
            count++;

            if (id == 0) {
               // sql = "UPDATE linksperdataset SET count='" + count + "' WHERE id='" + id + "'";
                sql = "UPDATE linksperdataset SET count=? WHERE id=?";
                statement = connect.prepareStatement(sql);
                statement.setInt(1, count);
                statement.setInt(2, id);


            } else {

               // sql = "INSERT INTO linksperdataset (uripld,uripred,count) VALUES ('" + dataset + "','" + predicate + "','" + count + "')";
                sql = "INSERT INTO linksperdataset (uripld,uripred,count) VALUES (?,?,?)";
                statement = connect.prepareStatement(sql);
                statement.setString(1, dataset);
                statement.setString(2, predicate);
                statement.setInt(3, count);

            }


            statement.executeUpdate();


        } catch (Exception e) {
            log.info(e.getMessage());
        }
        finally {
            try {
                rs.close();
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
*/
    public void readPredicates(String predicatesFilePath) {
        Connection connect = null;

        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");
            // Setup the connection with the DB
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/"+ConfigurationManager.getInstance().getDBName()+"?"
                            + "user="+ConfigurationManager.getInstance().getDBUserName()+"&password="+ConfigurationManager.getInstance().getDBPassword()+"");

            ZipFile zip = null;
            try {
                zip = new ZipFile(predicatesFilePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
            InputStream in = null;
            try {
                in = zip.getInputStream(zip.getEntry("predicates.txt"));
            } catch (IOException e) {
                e.printStackTrace();
            }


            try {
                InputStreamReader inR = new InputStreamReader(in);
                BufferedReader buf = new BufferedReader(inR);
                String line;
                while ((line = buf.readLine()) != null) {
                    insertPredicate(line, connect);

                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            log.info(e.getMessage().toString());
        } finally {
            try {

                connect.close();
            } catch (SQLException se) {
            }// do nothing
            try {
                if (connect != null)
                    connect.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }//end finally try
        }

    }

    public void importLinks() {


        try {


            // log.info("logging on");
            InputStream in = null;
            if(this.linksFilePath.endsWith(".zip")) {
                ZipFile zip = null;
                try {
                    zip = new ZipFile(this.linksFilePath);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                //TODO: files path

                try {
                    in = zip.getInputStream(zip.getEntry("links.nq"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if(this.linksFilePath.endsWith(".nq"))
            {
               try {
                   in = new FileInputStream(this.linksFilePath);
               }
               catch (IOException e) {
                   e.printStackTrace();
               }
            }

            String sql = "INSERT INTO alllinks(source,predicate,target, datasets,typelink, datasetspld, datasettpld,datasetcpld) VALUES (?,?,?,?,?,?,?,?)";
            statement = connect.prepareStatement(sql);


            NxParser nxp = new NxParser();
            nxp.parse(in);

            connect.setAutoCommit(false);

            for (Node[] nx : nxp) {

                String subject = nx[0].getLabel();
                String predicate = nx[1].getLabel();
                String object = nx[2].getLabel();
                String context = nx[3].getLabel();


                //redundance in uri - predicate column but is OK

                // find out what the data sets are



                insertLink(subject, predicate, object, context, connect);
                //addPredsPerDataset(predicate, context, connect);

                // add the data set info


            }
            statement.executeBatch();
            connect.commit();
            statement.clearBatch();
            statement.clearParameters();

        } catch (Exception e) {
            log.info(e.getMessage().toString());
            try {
                connect.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

        } finally {
            try {

                connect.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }// do nothing

        }
    }

    /*public void processLinksTDB() {
        //TODO:Links path
        String assemblerFile = "file:///C:/Users/csarasua/Documents/Starfish/tdb.ttl";

        ZipFile zip = null;
        try {
            zip = new ZipFile(this.linksFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //TODO: files path
        InputStream in = null;
        try {
            in = zip.getInputStream(zip.getEntry("links.nq"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        org.apache.jena.query.Dataset dataset = TDBFactory.createDataset("target/TDBlinks");


        try {
            dataset.begin(ReadWrite.WRITE);
            Model model = dataset.getDefaultModel();


            model.read(in, null, "NQuads");
            dataset.commit();
        } catch (Exception e) {
            e.printStackTrace();
            dataset.abort();
        } finally {
            dataset.end();
        }

        try {
            dataset.begin(ReadWrite.READ);


            String queryString = "DESCRIBE <http://wifo5-04.informatik.uni-mannheim.de/factbook/data/Netherlands>";

            QueryExecution qe = QueryExecutionFactory.create(queryString, dataset);
            Model results = qe.execDescribe();
            StmtIterator stIt = results.listStatements();
            while (stIt.hasNext()) {
                org.apache.jena.rdf.model.Statement st = stIt.nextStatement();
                log.info("statement in the describe results: " + st.getSubject().getURI() + " " + st.getPredicate().getURI() + "" + st.getObject().asResource().getURI());
            }
            dataset.commit();

        } catch (Exception e) {
            e.printStackTrace();
            dataset.abort();
        } finally {
            dataset.end();
        }


    }*/

    private String pld(String URI) {
        String pld = null;

        StringBuffer pldTemp = new StringBuffer();

        String[] URIparts = URI.split("/");
        int partsL = 0;

        for (int i = 0; i < URIparts.length; i++) {
            if (i != 0) {
                pldTemp.append("/");
            }
            pldTemp.append(URIparts[partsL]);

            if (this.datasets.containsKey(pldTemp.toString())) {
                pld = new String(pldTemp.toString());
                break;
            }
            partsL++;
        }
        if (pld == null) {
            pldTemp = new StringBuffer();

            pldTemp.append(URIparts[0]);
            pldTemp.append("/");
            pldTemp.append(URIparts[1]);
            pldTemp.append("/");
            pldTemp.append(URIparts[2]);
            pldTemp.append("/");
            pld = new String(pldTemp.toString());
        }


        return pld;
    }

}
