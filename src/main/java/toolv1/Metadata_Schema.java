package toolv1;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Metadata_Schema {
    private static String file_location = "C:/Users/ocraw/Desktop/meta.txt";
    private static Map<String, String> metadata;

    Metadata_Schema() {
        metadata = new HashMap<>();
    }

    public static void getAllFieldsByType(String type) {
        try {
            FileInputStream fis = new FileInputStream(file_location);

            //Construct BufferedReader from InputStreamReader
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));

            String line = null;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("###")) {
                    line = line.substring(3);
                    if (line.equals(type)) {
                        // do something clever here...
                    }
                }
            }

            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void addLabelProps(String nodeLabel, String metaEntry) {
        metadata.put(nodeLabel, metaEntry);
    }

    void createFile() {
        PrintWriter writer;
        try {
            writer = new PrintWriter(file_location, "UTF-8");
            for (String s : metadata.keySet()) {
                writer.println("###" + s);
                for (String s2 : metadata.get(s).split(", ")) {
                    writer.println(s2);
                }
            }
            writer.close();
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
