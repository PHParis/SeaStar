package de.unikoblenz.west.seastar.datapreparation;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by csarasua.
 */
public class Links2LinksetsGenerator {

    File allLinks;
    File linksetLinks;

    public Links2LinksetsGenerator(File fIn, File fOut)
    {
        this.allLinks=fIn;
        this.linksetLinks=fOut;

        //Get the catalogue of datasets

        //Create the Map of links by source data sets




    }
    //to be deleted
    public void generateSubset()
    {
        InputStream in=null;
        try {
            Files.write("", this.linksetLinks, Charsets.UTF_8);

            in = new FileInputStream(this.allLinks.getAbsoluteFile());
        } catch (IOException e) {
            e.printStackTrace();
        }


        NxParser nxp = new NxParser();
        nxp.parse(in);

        int count = 0;

        for (Node[] nx : nxp) {


            String subject = nx[0].getLabel();
            String predicate = nx[1].getLabel();
            String object = nx[2].getLabel();
            String context = nx[3].getLabel();


            //ignore some of the other classification triples (e.g. related to FOAF)
            if(context.startsWith("http://data.dcs.shef.ac.uk/") || (context.startsWith("http://bibliontology.com/bibo/")))
            {
                try {
                    Files.append("<" + subject + "> <" + predicate + "> <" + object + "> <" + context + "> .", this.linksetLinks, Charsets.UTF_8);
                    String ls = System.getProperty("line.separator");
                    Files.append(ls, this.linksetLinks, Charsets.UTF_8);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }



        // Each Entry of the Map has

    }








}
