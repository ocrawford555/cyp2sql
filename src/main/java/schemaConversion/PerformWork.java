package schemaConversion;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

class PerformWork implements Runnable {
    private ArrayList<String> lines;
    private String fileExt;
    private BufferedWriter bwNodes;
    private BufferedWriter bwEdges;

    PerformWork(ArrayList<String> strings, int id, String file) {
        this.lines = strings;
        this.fileExt = file;

        FileOutputStream fosNodes;
        FileOutputStream fosEdges;
        try {
            fosNodes = new FileOutputStream(
                    SchemaTranslate.nodesFile.replace(".txt", this.fileExt + ".txt"));
            //Construct BufferedReader from InputStreamReader
            this.bwNodes = new BufferedWriter(new OutputStreamWriter(fosNodes));

            fosEdges = new FileOutputStream(
                    SchemaTranslate.edgesFile.replace(".txt", this.fileExt + ".txt"));
            //Construct BufferedReader from InputStreamReader
            this.bwEdges = new BufferedWriter(new OutputStreamWriter(fosEdges));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        Set<Map.Entry<String, JsonElement>> entries;
        Matcher m;

        for (String s : lines) {
            // remove CREATE characters
            s = s.substring(7).toLowerCase();

            //using regex to decide between node or relationship
            m = SchemaTranslate.patternN.matcher(s);

            // is a node
            if (m.find()) {
                // firstSplit[0] contains id and node label
                // firstSplit[1] contains properties of the node
                String[] firstSplit = s.split("` ");

                String[] idAndTable = firstSplit[0].split(":`");
                int id = Integer.parseInt(idAndTable[0].substring(2));

                // TODO: fix for the large movies parsing - need to fix for multiple labels issues
                for (int i = 2; i < idAndTable.length; i++) {
                    idAndTable[1] += idAndTable[i];
                    //hack to remove extra labels
                    i += 8;
                }

                String nodeLabel;
                idAndTable[1] = idAndTable[1].replace("`", "");

                if (idAndTable[1].contains("person")) {
                    nodeLabel = idAndTable[1].substring(6, idAndTable[1].length());
                } else
                    nodeLabel = idAndTable[1].substring(0, idAndTable[1].length());

                String props = firstSplit[1].replace("`", "");

                JsonObject o = (JsonObject) SchemaTranslate.parser.parse(props.substring(0, props.length() - 1));

                o.addProperty("id", id);
                o.addProperty("label", nodeLabel);

                entries = o.entrySet();
                for (Map.Entry<String, JsonElement> entry : entries) {
                    if (!SchemaTranslate.nodeRelLabels.contains(entry.getKey() + " TEXT") &&
                            !SchemaTranslate.nodeRelLabels.contains(entry.getKey() + " INT")) {
                        try {
                            Integer.parseInt(entry.getValue().getAsString());
                            SchemaTranslate.nodeRelLabels.add(entry.getKey() + " INT");
                        } catch (NumberFormatException nfe) {
                            SchemaTranslate.nodeRelLabels.add(entry.getKey() + " TEXT");
                        }
                    }
                }


                try {
                    bwNodes.write(o.toString());
                    bwNodes.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                // relationship to add to SQL
                s = s.replace("`", "");

                //items[0] is left part of relationship
                //items[1] is relationship identifier
                //items[2] is the right part (has direction in example but ignoring currently)
                String[] items = s.split("\\)-");

                int idL = Integer.parseInt(items[0].substring(2, items[0].length()));

                String[] innerItems = items[1].split("->");
                int idR = Integer.parseInt(innerItems[1].substring(2, innerItems[1].length() - 1));

                String relationship = innerItems[0].substring(2, innerItems[0].length() - 1);

                // does the relationship have properties
                m = SchemaTranslate.patternR.matcher(s);

                JsonObject o = null;

                if (m.find()) {
                    String[] relAndProps = relationship.split(" \\{");
                    relationship = relAndProps[0];
                    relAndProps[1] = "{".concat(relAndProps[1]);
                    o = (JsonObject) SchemaTranslate.parser.parse(relAndProps[1]);
                }

                if (o == null) o = new JsonObject();

                o.addProperty("idL", idL);
                o.addProperty("idR", idR);
                o.addProperty("type", relationship);

                entries = o.entrySet();
                for (Map.Entry<String, JsonElement> entry : entries) {
                    if (!SchemaTranslate.edgesRelLabels.contains(entry.getKey() + " TEXT") &&
                            !SchemaTranslate.edgesRelLabels.contains(entry.getKey() + " INT")) {
                        try {
                            Integer.parseInt(entry.getValue().getAsString());
                            SchemaTranslate.edgesRelLabels.add(entry.getKey() + " INT");
                        } catch (NumberFormatException nfe) {
                            SchemaTranslate.edgesRelLabels.add(entry.getKey() + " TEXT");
                        }
                    }
                }

                try {
                    bwEdges.write(o.toString());
                    bwEdges.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            bwNodes.close();
            bwEdges.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
