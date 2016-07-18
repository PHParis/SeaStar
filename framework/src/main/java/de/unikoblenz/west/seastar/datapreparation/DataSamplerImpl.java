package de.unikoblenz.west.seastar.datapreparation;

import de.unikoblenz.west.seastar.utils.ConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by csarasua.
 * <p>
 * Class that gets a sample of the total set of links from the database and copies it into a CSV file.
 */
public class DataSamplerImpl implements DataSampler {

    // sample entities
    Connection connect;
    private static final Logger log = LoggerFactory.getLogger(DataSamplerImpl.class);


    public DataSamplerImpl() {

        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();
        java.util.logging.Logger.getLogger("global").setLevel(Level.WARNING);

        try {
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/" + ConfigurationManager.getInstance().getDBName() + "?"
                            + "user=" + ConfigurationManager.getInstance().getDBUserName() + "&password=" + ConfigurationManager.getInstance().getDBPassword());
        } catch (Exception e) {
            log.error("error connecting database ", e);
        }
    }


    public void extractLinksCompleteSample(int sizeOfSample, boolean byType) {


        String sql = "INSERT INTO starfish.randomexplinks ( SELECT * FROM starfish.explinks ORDER BY RAND() LIMIT "+sizeOfSample+")";
        try {
            Statement statement = connect.createStatement();


            int r = statement.executeUpdate(sql);



        } catch (Exception e) {
            log.error("opening connection ",e);
        }
    }

    public void extractLinksByEntityCompleteSample(int sizeOfSample, boolean byType) {


        String sql = "INSERT INTO starfish.randomexplinks (SELECT id,L1.source,predicate,target,datasets,datasett,typelink,datasetspld,datasettpld FROM starfish.explinks AS L1 RIGHT JOIN (SELECT source FROM starfish.explinks GROUP BY source ORDER BY RAND() LIMIT "+sizeOfSample+") AS L2 ON L1.source = L2.source)";
        try {
            Statement statement = connect.createStatement();


            int r = statement.executeUpdate(sql);



        } catch (Exception e) {
            log.error("opening connection ",e);
        }
    }



    public void extractLinksSample (Set < String > namespacesOfDataset,int sizeOfSample, boolean byType)
        {

        }
    }
