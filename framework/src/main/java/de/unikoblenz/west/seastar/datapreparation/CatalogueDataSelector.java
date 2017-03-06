package de.unikoblenz.west.seastar.datapreparation;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.runner.Result;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.HashMap;

/**
 * Created by csarasua.
 */
public class CatalogueDataSelector {

    Connection connect;
    Statement statement;
    PreparedStatement preparedStatement;

    String workingDir= System.getProperty("user.dir");
    String workingDirForFileName= workingDir.replace("\\", "/");



    HashMap<String,Boolean> datasetNames = new HashMap<String,Boolean>();

    public CatalogueDataSelector()
    {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();

            // local MySQL in laptop
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost:3306/seastar" + "?user=root&password=OHlala2%25");



        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void selectDataSets() {

        // Read Dataset Catalogue
        Reader in = null;
        try {
            in = new FileReader(this.workingDirForFileName+"/src/main/resources/datasetsAndCategories_h2d.tsv");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Iterable<CSVRecord> records = null;
        try {
            records = CSVFormat.TDF.withHeader("dataset", "category").parse(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //TODO: //Load the count lines file in the database from uploads blabla
        for (CSVRecord record : records) {


            Marker m = MarkerFactory.getMarker("debug");
            //log.isDebugEnabled(m);
            //log.info("logging on");

            String datasetPLD = record.get("dataset");
        //    String[] protocolSplit = datasetPLD.split("//");


           // String replaceName = protocolSplit[1].replaceAll("\\.","-");


           this.datasetNames.put(datasetPLD,true); // with . and http:// name
            String host = datasetPLD;
            try {
                URI uri = new URI(datasetPLD);
                String hostn =uri.getHost();
                host = hostn.replaceAll("\\.","-");
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }

            /*
            String firstPart = protocolSplit[1];
            String[] nameSplit = firstPart.split("\\.");
            String name = nameSplit[0];
            */


            // TODO: get the middle name


            // For each entry in the dataset catalogue look for the name in the domain



            updateDB(host);




        }

/*
        String checkDataSets= "SELECT nameFile FROM datasets";
        try {

            statement = connect.createStatement();
            ResultSet res = statement.executeQuery(checkDataSets);
            while (res.next())
            {
                String nameFileExtracteFromSplit = res.getString("nameFile");

                String[] nameFS = nameFileExtracteFromSplit.split("_");
                String name = nameFS[1].replace(".nt","");
                String rightName = name.replaceAll("-",".");

                String n = "http://"+rightName;
                if (datasetNames.containsKey(n))
                {
                    updateDB(nameFileExtracteFromSplit );
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        */

    }

        private void updateDB(String name)
    {
//WHERE nameFile=?";
       // String checkDataSets= "UPDATE datasets SET isInCatalogue=1 WHERE nameFile=?";
        String checkDataSets = "UPDATE datasets SET isInCatalogue=1,associatedPLD=? WHERE nameFile LIKE ?";
        try {
            preparedStatement = connect.prepareStatement(checkDataSets);
            preparedStatement.setString(1,name);
            preparedStatement.setString(2,"%"+name+"%");
           // preparedStatement.setString(1,name);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
