package de.unikoblenz.west.seastar.utils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;
/**
 * @author csarasua
 */
public class ConfigurationManager {

    private Configuration config;

    static private ConfigurationManager singleton;

    private ConfigurationManager() {

        try {

            String workingDir = System.getProperty("user.dir");
            File f = new File(workingDir + "/config.properties");
            config = new PropertiesConfiguration(f);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static synchronized ConfigurationManager getInstance() {

        if (singleton == null) {

            singleton = new ConfigurationManager();

        }
        return singleton;
    }

    public int getA() {
        return config.getInt("a");
    }

    public void setA(String a) {

    }

    public String getDBName()
    {return this.config.getString("db.name");}
    public String getDBUserName()
    {return this.config.getString("db.username");}
    public String getDBPassword()
    {return this.config.getString("db.password");}
}
