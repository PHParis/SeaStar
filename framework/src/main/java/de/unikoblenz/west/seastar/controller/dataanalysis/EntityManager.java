package de.unikoblenz.west.seastar.controller.dataanalysis;

import de.unikoblenz.west.seastar.model.Dataset;
import de.unikoblenz.west.seastar.model.TypeOfDatasetLocation;
import de.unikoblenz.west.seastar.trash.BasicDescriptiveStatsAnalyzer;
import de.unikoblenz.west.seastar.utils.Constants;
import de.unikoblenz.west.seastar.utils.URIUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RiotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by csarasua.
 */
public class EntityManager {


    Dataset dataset;

    private static final Logger log = LoggerFactory.getLogger(BasicDescriptiveStatsAnalyzer.class);



    public EntityManager(Dataset dataset){


        this.dataset=dataset;
    }


    public Model getEntityModelFromDataset(String entity)
    {

        Model resultModel = ModelFactory.createDefaultModel();
        Resource entityResource = resultModel.createResource(entity);

        ResultSet results=null;
        QueryExecution qexec=null;
        QuerySolution qs=null;

        String queryString="SELECT * WHERE{<"+entity+"> ?predicate ?object}";

         if (this.dataset.getTypeOfLocation().equals(TypeOfDatasetLocation.SPARQLENDPOINT)) {


         Query query = QueryFactory.create(queryString);
         qexec = QueryExecutionFactory.sparqlService(this.dataset.getLocation(), query);

         results = qexec.execSelect();
         }
         else if (this.dataset.getTypeOfLocation().equals(TypeOfDatasetLocation.FILEDUMP)) {
         qexec = QueryExecutionFactory.create(queryString, this.dataset.getModel());
         results = qexec.execSelect();

         }


         while (results.hasNext()) {
         qs = results.nextSolution();

         Resource pred = qs.getResource("predicate");
         Property predicate = resultModel.createProperty(pred.getURI());

         RDFNode object = qs.get("object");
         entityResource.addProperty(predicate,object);

         }

        return resultModel;



    }


    public void getAllModels(Model inputSourceModel, Model classificationModel, Model descModel, Model connectivityModel)
    {
        Model resultModel = ModelFactory.createDefaultModel();

         // remove
        // links existing in the description - not rdf:type statements there
        StmtIterator stIt = resultModel.listStatements();
        while(stIt.hasNext())
        {
            Statement st =  stIt.next();
            String pldSubject=null;
            String pldObject = null;

            Resource subject =st.getSubject().asResource();
            Property predicate = st.getPredicate();
            RDFNode object =st.getObject();



            if(predicate.equals(Constants.NS_RDF+"type"))
            {
                Statement stToAdd = classificationModel.createStatement(subject,predicate,object);
                classificationModel.add(stToAdd);
            }
            else{

                Statement stToAdd = descModel.createStatement(subject,predicate,object);
                descModel.add(stToAdd);

                if(object.isURIResource()) // includes mailto
                {
                    Statement stToAddc = connectivityModel.createStatement(subject,predicate,object);
                    connectivityModel.add(stToAddc);
                }
            }



        }
    }



    public Model getEntityConnectionModel(Model m)
    {

        Model resultModel = ModelFactory.createDefaultModel();

        resultModel.add(m);

        // remove
        // links existing in the description - not rdf:type statements there
        StmtIterator stIt = resultModel.listStatements();
        while(stIt.hasNext())
        {
            Statement st =  stIt.next();
            String pldSubject=null;
            String pldObject = null;
            RDFNode subject =st.getSubject();
            Property predicate = st.getPredicate();
            RDFNode object =st.getObject();



            if(predicate.equals(Constants.NS_RDF+"type")||object.isLiteral()||object.isAnon())
            {
                // then remove from "original description because it is a link"
                resultModel.remove(st);
            }


        }

        // remove all different plds or remove only some links like foaf:primtopic no?


        return resultModel;
    }


    /**
     *
     * Input: list of links of picked up entities in a CSV file
     * format: source,predicate,target,context,datasetspld,datasettpld
     * Output: output file like the diversity, with type of entity
     * format:
     */


    public Model getEntityModelDeref(String URI)
    {

        Model m = ModelFactory.createDefaultModel();
        Model m2 = ModelFactory.createDefaultModel();
        HttpClient client = new DefaultHttpClient();
        HttpGet getItemRDF=null;

        //HttpGet getFeedRecentChanges = new HttpGet("https://www.wikidata.org/entity/"+itemWikidataID);
        try {
            getItemRDF = new HttpGet(URI);

            getItemRDF.setHeader("Accept", "application/rdf+xml");

            HttpResponse response = null;

            try {
                response = client.execute(getItemRDF);

                int statusCode = response.getStatusLine().getStatusCode();
                if ((statusCode == 200) || (statusCode == 300)) { // 303 see other
                    // Header header = response.getEntity().getContentType();




                    InputStream rdfUri = response.getEntity().getContent();


                    String responseAsString = IOUtils.toString(rdfUri, org.apache.commons.io.Charsets.UTF_8);

                    InputStream in = new ByteArrayInputStream(responseAsString.getBytes("UTF-8"));
                    if (responseAsString.contains("<rdf:RDF")) // in the target we may have URLs as value, which are not RDF resources

                    {
                        System.out.println("URI: " + rdfUri);
                        try {
                            m.read(in, "RDF/XML");
                        }
                        catch(RiotException e) // this exception is shown when the model has no statement
                        {
                            log.error("Error when reading the RDF model of "+getItemRDF,e);

                        }



                        System.out.println("URI of entity whose model is being looked up: " + URI);
                        m2 = getOriginalModel(m);
                    }

                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }

        }
        catch(IllegalArgumentException e)
        { log.error("Illegal argument exception in looked up URI:"+getItemRDF,e);}


        try {
            Model m3 = ModelFactory.createDefaultModel();
            m3.add(m2);
            StmtIterator stIt = m2.listStatements();
            while (stIt.hasNext()) {
                Statement st = stIt.nextStatement();
                if (!st.getSubject().isURIResource() || !st.getSubject().getURI().equals(URI)) {
                    m3.remove(st);
                }
            }
        }catch (Exception e)
        {
            e.printStackTrace();
        }

        return m2;

    }

    public Model getOriginalModel(Model m)
    {
        Model m2=ModelFactory.createDefaultModel();
        m2.add(m);

        // remove
        StmtIterator stIt = m.listStatements();
        while(stIt.hasNext())
        {
            Statement st =  stIt.next();
            String pldSubject=null;
            String pldObject = null;
            RDFNode subject =st.getSubject();
            Property predicate = st.getPredicate();
            RDFNode object =st.getObject();


            if(subject.isURIResource())
            {
                pldSubject = URIUtils.pld(subject.asResource().getURI());
            }


            if(object.isURIResource() && !object.toString().startsWith("mailto"))
            {
                pldObject = URIUtils.pld(object.asResource().getURI());
            }

            if(pldSubject!=null && pldObject!=null &&!pldSubject.equals(pldObject)&& !st.getPredicate().equals(Constants.NS_RDF+"type"))
            {
                // then remove from "original description because it is a link"
                m2.remove(st);
            }


        }

        // remove all different plds or remove only some links like foaf:primtopic no?

        return m2;
    }




}
