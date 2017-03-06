package de.unikoblenz.west.seastar.datapreparation;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.LineReader;
import de.unikoblenz.west.seastar.model.Dataset;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RiotException;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by csarasua.
 */
public class LDCrawlDataSelection {

    String filePath;
    String outputFilePath;
    String erroneousURIsFilePath;

    File output;
    File errors;

    BufferedWriter writer = null;

    Dataset dataset;

    private static final Logger log = LogManager.getLogger(LDCrawlDataSelection.class);



    /**
     * @param pathD  path of file with the data set
     * @param pathSD path of output file for selected data
     * @param pathE  path of output file for erroneous URIs
     * could parametrize the set of namespaces too
     */
    public LDCrawlDataSelection(String pathD, String pathSD, String pathE ) {


        this.filePath = pathD;



        this.filePath = pathD;
        this.outputFilePath = pathSD;
        this.erroneousURIsFilePath = pathE;

        output = new File(outputFilePath);
        errors = new File(erroneousURIsFilePath);

        //to enter it in TB directly, to see statement by statement what is good or not, but still generates the clean file

       // dataset = new Dataset("complete dump mannheim", TypeOfDatasetLocation.FILEDUMP, "E:/PHD/starfishdata/mycleandump.nq.gz", null, null, null);


        try {
            Files.write("", new File(this.outputFilePath), Charsets.UTF_8);
            Files.write("", new File(this.erroneousURIsFilePath), Charsets.UTF_8);



                GZIPOutputStream zip = new GZIPOutputStream(
                        new FileOutputStream(new File("E:/PHD/starfishdata/mycleandump.nq.gz"))); //"mydum.zip"

                writer = new BufferedWriter(
                        new OutputStreamWriter(zip, "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }




    }

    public void generateSubsetOfExperimentDatasets() {

        String line = new String();
        GZIPInputStream in = null;
        GZIPInputStream in2 = null;

        Model m = ModelFactory.createDefaultModel();

        try {
            in = new GZIPInputStream(new FileInputStream(this.filePath));
            in2 = new GZIPInputStream(new FileInputStream(this.filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }



        LineReader lr = new LineReader(new InputStreamReader(in2));



        NxParser nxp = new NxParser();
        nxp.parse(in);

        int count = 0;

        for (Node[] nx : nxp) {

            try {
                line =  lr.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }

            // for testing

            /*
            if(count==1000000)
            {break;}
            */


            String subject = nx[0].getLabel();
            String predicate = nx[1].getLabel();
            String object = nx[2].getLabel();
            String context = nx[3].getLabel();

           // boolean relevant = isRelevant(subject, predicate, object, context);
          /*  if(count==755)
            {
                log.info("a");
            }*/



            if(subject.startsWith("http://")) {

                boolean correctURIs = correctURIs(subject, predicate, object, context);


                // subject can be blank node --> ignore them
                // object can be blank node or literal --> ignore blank node but write literal properly

                // if (relevant) {
                if (correctURIs) {

                /* with Guava directly in a .nq file
                try {
                    Files.append("<" + subject + "> <" + predicate + "> <" + object + "> <" + context + "> .", output, Charsets.UTF_8);
                    String ls = System.getProperty("line.separator");
                    Files.append(ls, output, Charsets.UTF_8);



                } catch (IOException e) {
                    log.error("problem generating links.nq ", e);
                }*/

                    // Directly on a GZIP

                    try {


                        String line2=line;
                        // writer.append("<" + subject + "> <" + predicate + "> <" + object + "> <" + context + "> .");
                       /* if(line.contains("\"@zh_pinyin"))
                        {
                           line2 = line.replace("zh_pinyin", "zh");
                        }*/




                        if(!line.contains("_:") && (line.contains("_")))
                        {
                            line2 = line.replace("_", "");
                        }
                        line = new String(line2);

/*
                        if(line.contains(". "))
                        {
                            line2 = line.replace(". ", "; ");
                        }
                        line = new String(line2);
*/


                        boolean correctRDF = true;
                       try {

                           Resource r=null;
                           if(subject.startsWith("http://"))
                           {
                               r=m.createResource(subject);
                           }
                           Property p=null;
                           if(predicate.startsWith("http://"))
                           {
                               p=m.createProperty(predicate);
                           }
                           Resource o=null;
                           Literal l=null;
                           Statement st=null;

                           if(object.startsWith("http://")||object.startsWith("_:"))
                           {

                              o = m.createResource(object);
                               r.addProperty(p,o);
                           }
                           else
                           {
                               l=m.createLiteral(object);
                               r.addProperty(p,l);
                           }


/*
                           PrintWriter writer = new PrintWriter(System.out);
                           writer.println("<?xml version='1.0' encoding='utf-8' standalone='no'?>");
                           writer.flush();
                           m.write(writer, "RDF/XML-ABBREV");
                           */

                           //writer.close();

                       }
                       catch(RiotException e)
                       {
                           System.out.println(e.getMessage());
                           correctRDF=false;
                       }
                        //make it empty
                        m = ModelFactory.createDefaultModel();
                        if(correctRDF) {
                            writer.append(line);
                            writer.newLine();
                        }




                    } catch (Exception e) {
                        log.error("problem generating links.nq ", e);
                    }

                }
            }

            count++;


        }

        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    public boolean correctURIs(String s, String p, String o, String c)
    {


        List<String> elements = new ArrayList<String>();
       elements.add(s);
        elements.add(p);
        elements.add(o);
        elements.add(c);

        Model m = ModelFactory.createDefaultModel();


        for (int i = 0; i < elements.size(); i++) {

            String el = new String(elements.get(i));

            //if (uri.startsWith("http://") ) // else is blank node
           // {
                try {




                    URI uriCheck = new URI(el);






                } catch (URISyntaxException e) {


                    try {
                        //Charset.defaultCharset()
                        Files.append(el, errors, Charsets.UTF_8);
                        String ls = System.getProperty("line.separator");
                        Files.append(ls, errors, Charsets.UTF_8);
                    } catch (IOException e1) {
                        e1.printStackTrace();
                        System.out.println("a");
                        return false;
                    }

                }
            catch(Exception e){return false;}
            //}
        }
        return true;
    }


    public boolean isRelevant(String s, String p, String o, String c) {


        List<String> uris = new ArrayList<String>();
        uris.add(s);
        uris.add(p);
        uris.add(o);


        if (c.contains("zbw.eu") || c.contains("data.semanticweb.org") || c.contains("lod2.eu") || c.contains("bibbase") ||
                c.contains("d-nb") || c.contains("transparency.270a") || c.contains("estatwrap.ontologycentral") || c.contains("openei") || c.contains("reegle")
                || c.contains("eea.europa") || c.contains("bbc.co.uk/nature") || c.contains("bbc.co.uk/programmes") || c.contains("data.nytimes.com") || c.contains("dbtune.org")
                || c.contains("bio2rdf.org") || c.contains("company.semantic-web") || c.contains("harth.org") ||
                c.contains("korrekt.org") || c.contains("tomheath.com") || c.contains("kit.edu")
                || c.contains("http://dbpedia.org") || c.contains("de.dbpedia") || c.contains("umbel.org") || c.contains("ontosearch.com") || c.contains("lexvo.org")
                || c.contains("linkedgeodata.org") || c.contains("nuts.geovocab") || c.contains("sws.geonames.org") || c.contains("www.geonames.org") || c.contains("london.randomness.org.uk") || c.contains("gadm.geovocab.org")) {


            for (int i = 0; i < uris.size(); i++) {

                String uri = new String(uris.get(i));

                if (uris.get(i).startsWith("http://")) // else is blank node
                {
                    try {
                        URI uriCheck = new URI(uri);


                    } catch (URISyntaxException e) {


                        try {
                            //Charset.defaultCharset()
                            Files.append(uri, errors, Charsets.UTF_8);
                            String ls = System.getProperty("line.separator");
                            Files.append(ls, errors, Charsets.UTF_8);
                        } catch (IOException e1) {
                            e1.printStackTrace();
                            System.out.println("a");
                        }

                    }
                } else {//System.out.println("no link!");
                    return false;
                }

            }

            // URIs are correct
            return true;
        }

        else
        {
            return false;
        }


    }

}
