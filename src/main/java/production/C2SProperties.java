package production;

import org.apache.commons.lang3.SystemUtils;

import java.io.*;
import java.util.Properties;

/**
 * Class for obtaining the correct properties of the tool from the associated properties file.
 */
class C2SProperties {
    private String cyp = null;
    private String pg = null;
    private String wspace = null;
    private String pun = null;
    private String ppw = null;
    private String nun = null;
    private String npw = null;
    private String lastDB = null;

    /**
     * Get the properties from the properties file.
     *
     * @return String[] with following index structure:
     * 0. location of the Cypher results text file.
     * 1. location of the Postgres results text file.
     * 2. location of the folder where intermediate work is stored (default /workspace).
     * 3. Postgres username
     * 4. Postgres password
     * 5. Neo4J username (usually neo4j)
     * 6. Neo4J password
     * 7. Name of the last database used by the tool (to correct SSL issues with Neo4J).
     */
    String[] getLocalProperties() {
        try {
            Properties prop = new Properties();

            // change where the props file is depending on the OS installation.
            String osPath;
            String propsFileName = "configC2S.properties";

            if (SystemUtils.IS_OS_LINUX) {
                osPath = "/home/ojc37/props/";
            } else {
                osPath = "C:/Users/ocraw/Desktop/";
            }
            prop.load(new FileInputStream(osPath + propsFileName));

            // get the property value and print it out
            cyp = prop.getProperty("cypherResultsLocation");
            pg = prop.getProperty("postgresResultsLocation");
            wspace = prop.getProperty("workspaceLocation");
            pun = prop.getProperty("postgresUser");
            ppw = prop.getProperty("postPW");
            nun = prop.getProperty("neo4JUser");
            npw = prop.getProperty("neoPW");
            lastDB = prop.getProperty("lastDatabase");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new String[]{cyp, pg, wspace, pun, ppw, nun, npw, lastDB};
    }
}
