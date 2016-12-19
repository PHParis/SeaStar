package de.unikoblenz.west.seastar.dataanalysis;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import de.unikoblenz.west.seastar.model.Dataset;
import de.unikoblenz.west.seastar.utils.ConfigurationManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.IOException;
import java.sql.*;


/**
 * Created by csarasua.
 */
public class DescriptiveStatsAnalyzer implements RawStatsAnalyzer{

    private static final Logger log = LogManager.getLogger(DescriptiveStatsAnalyzer.class);

    Connection connect;

    File basicStatsOutputFile;
    File logErrorsLinks;

    String workingDir;
    String workingDirForFileName;

    String typelinks;

    public DescriptiveStatsAnalyzer(Dataset dataset, String typelinks)
    {
        workingDir = System.getProperty("user.dir");
        workingDirForFileName = workingDir.replace("\\", "/");

        this.typelinks=typelinks;
        basicStatsOutputFile = new File(workingDirForFileName + "/output/basicStats_"+dataset.getTitle()+"_"+typelinks+".tsv");
        logErrorsLinks = new File(workingDirForFileName +"/output/error_basicStats_"+dataset.getTitle()+"_"+typelinks+".tsv");

        try {
            String ls = System.getProperty("line.separator");

            Files.write("", basicStatsOutputFile, Charsets.UTF_8);
            //writes header of result file
            Files.append("dataset" + "\t" + "numlinks" + "\t" + "numsourcentities" + "\t" + "linksets" + "\t" + "avgtargetedentities" + "\t" + "avglinkspersource" + "\t", this.basicStatsOutputFile, Charsets.UTF_8);
            Files.append(ls, this.basicStatsOutputFile, Charsets.UTF_8);
            Files.write("", logErrorsLinks, Charsets.UTF_8);
            try {
                Files.append(dataset.getTitle() + "\t", this.basicStatsOutputFile, Charsets.UTF_8);
            } catch (IOException e) {
                log.info(e.getStackTrace().toString());
            }
        } catch (IOException e) {
            log.info("problem connecting into the database ",e);
        }



        try {
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/" + ConfigurationManager.getInstance().getDBName() + "?"
                            + "user=" + ConfigurationManager.getInstance().getDBUserName() + "&password=" + ConfigurationManager.getInstance().getDBPassword());
        } catch (Exception e) {
            log.error("error connecting database ", e);
        }

    }
    public void computeStats(String dataset,  String namespace)
    {
        Statement statement = null;
        Statement statement2=null;
        PreparedStatement pStatement=null;



        try{
        // M1, M2
        String sql2 = "SELECT COUNT(distinct id) AS countl, COUNT(distinct source) as counte FROM starfish.alllinks WHERE datasetspld=? AND datasetcpld=? AND datasettpld<>? AND typelink=? ";
        pStatement = connect.prepareStatement(sql2);
        pStatement.setString(1, namespace);
        pStatement.setString(2, namespace);
        pStatement.setString(3, namespace);
        pStatement.setString(4, typelinks);
        //pStatement.setString(1, "%"+dataset+"%");
       //pStatement.setString(2, "%"+dataset+"%");

        ResultSet rs2 = pStatement.executeQuery();
        if (rs2.next()) {
            int countLinks = rs2.getInt("countl");
            int countEntities = rs2.getInt("counte");
            try {
                Files.append(countLinks + "\t" + countEntities + "\t", this.basicStatsOutputFile, Charsets.UTF_8);
            } catch (IOException e) {
                log.info(e.getStackTrace().toString());
            }
        }


        // M3 linksets
        sql2 = "SELECT C.predicate, C.co FROM (SELECT predicate, COUNT(distinct id) AS co FROM starfish.alllinks WHERE datasetspld=? AND datasetcpld=? AND datasettpld<>? AND typelink=? GROUP BY predicate) AS C";
        pStatement = connect.prepareStatement(sql2);
            pStatement.setString(1, namespace);
            pStatement.setString(2, namespace);
            pStatement.setString(3, namespace);
            pStatement.setString(4, typelinks);


            rs2 = pStatement.executeQuery();
        String linkSets = "(";
        while (rs2.next()) {
            String predicate = rs2.getString(1);
            int count = rs2.getInt(2);
            linkSets = "<"+linkSets+ predicate + "," + String.valueOf(count)+">";

        }
        linkSets = linkSets + ")";
        try {
            Files.append(String.valueOf(linkSets) + "\t", this.basicStatsOutputFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("error writing in output file ", e);
        }

        // M4 - M5
        sql2 = "SELECT AVG(C.co)FROM (SELECT COUNT(distinct target) AS co FROM starfish.alllinks WHERE datasetspld=? AND datasetcpld=? AND datasettpld<>? AND typelink=? GROUP BY source) AS C";
        pStatement = connect.prepareStatement(sql2);
            pStatement.setString(1, namespace);
            pStatement.setString(2, namespace);
           pStatement.setString(3, namespace);
            pStatement.setString(4, typelinks);


            rs2 = pStatement.executeQuery();
        double avgTargetedEntities = 0.0;
        if (rs2.next()) {
            avgTargetedEntities = rs2.getDouble(1);
        }
        try {
            Files.append(String.valueOf(avgTargetedEntities) + "\t", this.basicStatsOutputFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("error writing in output file ", e);
        }


        sql2 = "SELECT AVG(C.co)FROM (SELECT COUNT(distinct id) AS co FROM starfish.alllinks WHERE datasetspld=? AND datasetcpld=? AND datasettpld<>? AND typelink=? GROUP BY source) AS C";
        pStatement = connect.prepareStatement(sql2);
            pStatement.setString(1, namespace);
            pStatement.setString(2, namespace);
            pStatement.setString(3, namespace);
            pStatement.setString(4, typelinks);


            rs2 = pStatement.executeQuery();
        double avgLinks = 0.0;
        if (rs2.next()) {
            avgLinks = rs2.getDouble(1);
        }
        try {
            Files.append(String.valueOf(avgLinks) + "\t", this.basicStatsOutputFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("error writing in output file ", e);
        }



            String ls = System.getProperty("line.separator");


        try {
            Files.append(ls, this.basicStatsOutputFile, Charsets.UTF_8);
        } catch (IOException e) {
            log.error("error writing in outputfile ", e);
        }



} catch (SQLException e) {
        log.error("Error executing queries against the data base in basic descriptive stats ", e);

        }
        finally{
        try {
        pStatement.close();
        //statement.close();
        //statement2.close();
        } catch (SQLException e) {
        e.printStackTrace();
        }

        }
    }


    public void closeOpenConnections() {
        try {
            connect.close();
        } catch (SQLException e) {
            log.error("problem closing the connectinon to the database in BasicDescriptiveStatsAnalyzer", e);
        }
    }

}
