package de.unikoblenz.west.seastar.datapreparation;

import com.google.common.net.InternetDomainName;
import de.unikoblenz.west.seastar.utils.ConfigurationManager;
import de.unikoblenz.west.seastar.utils.URIUtils;

import java.net.URI;
import java.sql.*;

/**
 * Created by csarasua.
 */
public class Graph2DatasetMatcher {

    Connection readConnect;
    Connection writeConnect;
    Statement statement;
    PreparedStatement preparedStatement;

    public Graph2DatasetMatcher()
    {
        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");
            // Setup the connection with the DB
            readConnect = DriverManager
                    .getConnection("jdbc:mysql://localhost/" + ConfigurationManager.getInstance().getDBName() + "?"
                            + "user=" + ConfigurationManager.getInstance().getDBUserName() + "&password=" +ConfigurationManager.getInstance().getDBPassword() + "");

            writeConnect = readConnect = DriverManager
                    .getConnection("jdbc:mysql://localhost/" + ConfigurationManager.getInstance().getDBName() + "?"
                            + "user=" + ConfigurationManager.getInstance().getDBUserName() + "&password=" +ConfigurationManager.getInstance().getDBPassword() + "");


            URIUtils.loadDatasetCatalogue();

        }
        catch(Exception e)
        {e.printStackTrace();}
    }

    public void updateDatasetSTpld()
    {
        long count = 0;
        //where id - index much more efficient!
        //String updateplds="UPDATE alllinks SET datasetspld=?,datasettpld=?,datasetcpld=? WHERE id=?";
        String insertPlds = "INSERT INTO plds VALUES (?,?,?,?)"; // id, datasetspld, datasettpld, datasetcpld
        try {
            statement = readConnect.createStatement();

            boolean continueO=true;
            writeConnect.setAutoCommit(false);

            preparedStatement=writeConnect.prepareStatement(insertPlds);

            while(continueO) {
                String getLinks="SELECT id,source,predicate,target,datasets from alllinks LIMIT 1000 OFFSET "+count;


                continueO=false;
                ResultSet results = statement.executeQuery(getLinks);


                long counter = 0;


                while (results.next()) {
                    continueO = true;
                    String source = results.getString("source");
                    String predicate = results.getString("predicate");
                    String target = results.getString("target");
                    String context = results.getString("datasets");
                    int id = results.getInt("id");


                    String sourcePLD = URIUtils.pld(source);
                    String targetPLD = URIUtils.pld(target);
                    String contextPLD = URIUtils.pld(context);

                    preparedStatement.setInt(1, id);
                    preparedStatement.setString(2, sourcePLD);
                    preparedStatement.setString(3, targetPLD);
                    preparedStatement.setString(4, contextPLD);



                    preparedStatement.addBatch();
                    counter++;
                    if (counter % 1000 == 0) {
                        preparedStatement.executeBatch();
                        writeConnect.commit();


                        preparedStatement.clearBatch();
                    }


                }
                count=count+1000;

                preparedStatement.executeBatch();
                writeConnect.commit();
                preparedStatement.clearBatch();
            }

        } catch (SQLException e) {
            e.printStackTrace();
            try {
                writeConnect.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }

    }

    public void match()
    {

        // Read data sets
        String getDatasets="SELECT pld FROM datasets";

        try {
            statement = readConnect.createStatement();
            ResultSet results = statement.executeQuery(getDatasets);

            while(results.next())
            {
                String datasetCrawlURI = results.getString("pld");

                String realPLD="";
                try {
                    URI datasetURI = new URI(datasetCrawlURI);
                    realPLD = InternetDomainName.from(datasetURI.getHost()).topPrivateDomain().toString();
                    String tweekedRealPLD=realPLD.replace(".","-");
                    String updateDataset="UPDATE graphs SET dataset=? WHERE filename LIKE ?";
                    preparedStatement=readConnect.prepareStatement(updateDataset);
                    preparedStatement.setString(1,datasetCrawlURI);
                    preparedStatement.setString(2, "%"+tweekedRealPLD+"%");
                    preparedStatement.executeUpdate();
                }
                catch(Exception e)
                {

                }


            }


        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        // For each data set extract pay level domain with Guava

        // Update graphs adding the col of the URI of the data set where graph like PLD
    }
}
