package production;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

class C2SProperties {
    private String cyp = null;
    private String pg = null;
    private String wspace = null;
    private String pun = null;
    private String ppw = null;
    private String nun = null;
    private String npw = null;

    String[] getLocalProperties() {
        try {
            Properties prop = new Properties();
            String propFileName = "configC2S.properties";

            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            // get the property value and print it out
            cyp = prop.getProperty("cypherResultsLocation");
            pg = prop.getProperty("postgresResultsLocation");
            wspace = prop.getProperty("workspaceLocation");
            pun = prop.getProperty("postgresUser");
            ppw = prop.getProperty("postPW");
            nun = prop.getProperty("neo4JUser");
            npw = prop.getProperty("neoPW");

            inputStream.close();
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
        return new String[]{cyp, pg, wspace, pun, ppw, nun, npw};
    }
}
