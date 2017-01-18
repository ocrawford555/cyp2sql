package schemaConversion;

import com.google.gson.JsonArray;
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

/**
 * Each dump file is split into chunks which are operated on by individual threads using this
 * class.
 */
class PerformWork implements Runnable {
    private ArrayList<String> lines;
    private BufferedWriter bwNodes;
    private BufferedWriter bwEdges;

    PerformWork(ArrayList<String> strings, String file) {
        this.lines = strings;

        FileOutputStream fosNodes;
        FileOutputStream fosEdges;

        try {
            //Construct BufferedReader from InputStreamReader
            fosNodes = new FileOutputStream(SchemaTranslate.nodesFile.replace(".txt", file + ".txt"));
            this.bwNodes = new BufferedWriter(new OutputStreamWriter(fosNodes));

            //Construct BufferedReader from InputStreamReader
            fosEdges = new FileOutputStream(SchemaTranslate.edgesFile.replace(".txt", file + ".txt"));
            this.bwEdges = new BufferedWriter(new OutputStreamWriter(fosEdges));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Runnable methods that parses the Neo4J dump into a JSON representation that is used later on
     * when executing the new relations on Postgres.
     */
    public void run() {
        Set<Map.Entry<String, JsonElement>> entries;
        Matcher m;

        int totalLines = lines.size();
        int linesRead = 0;
        int previousPercent = 0;

        // temporary hack for the OPUS graphs where some names are lists and others are not.
        // intend to store in postgres as an array, and therefore single names not in list
        // format will be converted to a singleton list.
        boolean opusLists = false;

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

                for (int i = 2; i < idAndTable.length; i++) {
                    idAndTable[1] += idAndTable[i];
                }

                String nodeLabel;
                idAndTable[1] = idAndTable[1].replace("`", ", ");
                nodeLabel = idAndTable[1];
                if (nodeLabel.equals("process")) opusLists = true;

                String props = firstSplit[1].replace("`", "");

                JsonObject o = (JsonObject) SchemaTranslate.parser.parse(props.substring(0, props.length() - 1));

                if (o.has("name") && !o.get("name").isJsonArray()) {
                    String name = o.get("name").getAsString();
                    JsonArray j_array = new JsonArray();
                    j_array.add(name);
                    o.remove("name");
                    o.add("name", j_array);
                }

                o.addProperty("id", id);
                o.addProperty("label", nodeLabel);

                entries = o.entrySet();

                for (Map.Entry<String, JsonElement> entry : entries) {
                    addToLabelMap(nodeLabel, entry.getKey(), entry.getValue());

                    if (!SchemaTranslate.nodeRelLabels.contains(entry.getKey() + " TEXT") &&
                            !SchemaTranslate.nodeRelLabels.contains(entry.getKey() + " INT") &&
                            !SchemaTranslate.nodeRelLabels.contains(entry.getKey() + " BIGINT") &&
                            !SchemaTranslate.nodeRelLabels.contains(entry.getKey() + " TEXT[]")) {
                        if (entry.getValue().isJsonArray()) {
                            SchemaTranslate.nodeRelLabels.add(entry.getKey() + " TEXT[]");
                        } else {
                            try {
                                // another OPUS hack
                                if (entry.getKey().equals("mono_time")) throw new NumberFormatException();
                                Integer.parseInt(entry.getValue().getAsString());
                                SchemaTranslate.nodeRelLabels.add(entry.getKey() + " INT");
                            } catch (NumberFormatException nfe) {
                                try {
                                    Long.parseLong(entry.getValue().getAsString());
                                    SchemaTranslate.nodeRelLabels.add(entry.getKey() + " BIGINT");
                                } catch (NumberFormatException nfe2) {
                                    String textToAdd = entry.getKey() + " TEXT";
                                    SchemaTranslate.nodeRelLabels.add(textToAdd);
                                }
                            }
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

                if (!SchemaTranslate.relTypes.contains(relationship)) {
                    SchemaTranslate.relTypes.add(relationship);
                }

                entries = o.entrySet();


                for (Map.Entry<String, JsonElement> entry : entries) {
                    if (!SchemaTranslate.edgesRelLabels.contains(entry.getKey() + " TEXT") &&
                            !SchemaTranslate.edgesRelLabels.contains(entry.getKey() + " INT") &&
                            !SchemaTranslate.edgesRelLabels.contains(entry.getKey() + " BIGINT") &&
                            !SchemaTranslate.edgesRelLabels.contains(entry.getKey() + " TEXT[]")) {
                        try {
                            Integer.parseInt(entry.getValue().getAsString());
                            SchemaTranslate.edgesRelLabels.add(entry.getKey() + " INT");
                        } catch (NumberFormatException nfe) {
                            try {
                                Long.parseLong(entry.getValue().getAsString());
                                SchemaTranslate.edgesRelLabels.add(entry.getKey() + " BIGINT");
                            } catch (NumberFormatException nfe2) {
                                String textToAdd = entry.getKey() + " TEXT";
                                if (entry.getKey().equals("name") && opusLists) textToAdd = textToAdd + "[]";
                                SchemaTranslate.edgesRelLabels.add(textToAdd);
                            }
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

            linesRead++;
            int percent = (linesRead * 100 / totalLines);
            if (previousPercent < (percent - 5)) {
                System.out.println(percent + "% read.");
                previousPercent = percent;
            }
        }

        try {
            bwNodes.close();
            bwEdges.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addToLabelMap(String nodeLabel, String key, JsonElement value) {
        if (SchemaTranslate.labelMappings.keySet().contains(nodeLabel)) {
            String currentValue = SchemaTranslate.labelMappings.get(nodeLabel);

            if (!currentValue.contains(key)) {
                if (value.isJsonArray()) {
                    SchemaTranslate.labelMappings.put(nodeLabel, currentValue + ", " + key + " TEXT[]");
                } else {
                    String testValue = value.getAsString();
                    try {
                        // another OPUS hack
                        if (key.equals("mono_time")) throw new NumberFormatException();
                        Integer.parseInt(testValue);
                        SchemaTranslate.labelMappings.put(nodeLabel, currentValue + ", " + key + " INT");
                    } catch (NumberFormatException nfe) {
                        try {
                            Long.parseLong(testValue);
                            SchemaTranslate.labelMappings.put(nodeLabel, currentValue + ", " + key + " BIGINT");
                        } catch (NumberFormatException nfe2) {
                            String textToAdd = currentValue + ", " + key + " TEXT";
                            SchemaTranslate.labelMappings.put(nodeLabel, textToAdd);
                        }
                    }
                }
            }
        } else {
            if (value.isJsonArray()) {
                SchemaTranslate.labelMappings.put(nodeLabel, "id INT, " + key + " TEXT[]");
            } else {
                String testValue = value.getAsString();
                try {
                    Integer.parseInt(testValue);
                    SchemaTranslate.labelMappings.put(nodeLabel, "id INT, " + key + " INT");
                } catch (NumberFormatException nfe) {
                    try {
                        Long.parseLong(testValue);
                        SchemaTranslate.labelMappings.put(nodeLabel, "id INT, " + key + " BIGINT");
                    } catch (NumberFormatException nfe2) {
                        String textToAdd = "id INT, " + key + " TEXT";
                        SchemaTranslate.labelMappings.put(nodeLabel, textToAdd);
                    }
                }
            }
        }
    }
}
