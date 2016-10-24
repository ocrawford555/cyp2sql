package toolv1;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaTranslate {
    public static Map<String, String> readFile(String file, boolean DEBUG_PRINT, int i) throws Exception {
        if (i == 1) return methodOneSchema(file, DEBUG_PRINT);
        else if (i == 2) return methodTwoSchema(file, DEBUG_PRINT);
        else throw new Exception("3rd parameter incorrect");
    }

    private static Map<String, String> methodTwoSchema(String file, boolean DEBUG_PRINT) {
        // read through all lines in the text file containing the schema of the Cypher DB
        Map<String, String> returnSchema = new HashMap<>();

        // keep track of all the types of nodes seen so far
        ArrayList<String> nodeLabelList = new ArrayList<>();

        // keep track of all the types of nodes seen so far
        ArrayList<String> relsLabelList = new ArrayList<>();

        // regex for deciding whether a line is a node or a relationship
        String patternForNode = "(_\\d+:.*)";
        Pattern patternN = Pattern.compile(patternForNode);

        // regex for deciding whether relationship has properties
        String patternForRel = "\\{.+\\}";
        Pattern patternR = Pattern.compile(patternForRel);

        Matcher m;

        // JSON Parser for creating JSON objects from the text file.
        JsonParser parser = new JsonParser();

        boolean firstLineNodes = true;
        boolean firstLineRels = true;

        // create a class for the generation of the additional metadata file
        Metadata_Schema ms = new Metadata_Schema();

        try {
            for (String line : Files.readAllLines(Paths.get(file))) {
                // remove CREATE characters
                line = line.substring(7);

                //using regex to decide between node create or relationship create
                m = patternN.matcher(line);

                // is a node
                if (m.find()) {
                    // firstSplit[0] contains id and node label
                    // firstSplit[1] contains properties of the node
                    String[] firstSplit = line.split("` ");

                    String[] idAndTable = firstSplit[0].split(":");
                    String id = idAndTable[0].substring(2);
                    String nodeLabel = idAndTable[1].substring(1, idAndTable[1].length());

                    // remove bad characters from text file
                    String props = firstSplit[1].replace("`", "");

                    JsonObject o = parser.parse(props.substring(0, props.length() - 1)).getAsJsonObject();

                    // add to the metadata file
                    if (!nodeLabelList.contains(nodeLabel)) {
                        nodeLabelList.add(nodeLabel);
                        addToMetaData(ms, nodeLabel, o, 1);
                    }

                    if (DEBUG_PRINT)
                        System.out.println("NODE::  ID: " + id + "\tLABEL: " + nodeLabel + "\tPROPS: " + o.toString());

                    o.addProperty("id", Integer.parseInt(id));
                    o.addProperty("label", nodeLabel);

                    if (firstLineNodes) {
                        returnSchema.put("nodes", o.toString());
                        firstLineNodes = false;
                    } else {
                        String currentV = returnSchema.get("nodes");
                        returnSchema.put("nodes", currentV + ", " + o.toString());
                    }

                } else {
                    // relationship to add to SQL
                    line = line.replace("`", "");

                    //items[0] is left part of relationship
                    //items[1] is relationship identifier
                    //items[2] is the right part (has direction in example but ignoring currently)
                    String[] items = line.split("-");

                    String idL = items[0].substring(2, items[0].length() - 1);
                    String idR = items[2].substring(3, items[2].length() - 1);

                    JsonObject o = new JsonObject();
                    o.addProperty("idL", Integer.parseInt(idL));
                    o.addProperty("idR", Integer.parseInt(idR));

                    String relationship = items[1].substring(2, items[1].length() - 1);

                    //using regex to decide between node create or relationship create
                    m = patternR.matcher(line);

                    String jsonProps = null;

                    if (m.find()) {
                        String[] relAndProps = relationship.split(" \\{");
                        relationship = relAndProps[0];
                        relAndProps[1] = "{".concat(relAndProps[1]);
                        jsonProps = relAndProps[1];
                        o = addToExisitingJSON(o, jsonProps, parser, relationship);
                    }

                    o.addProperty("type", relationship);

                    // add to the metadata file
                    if (!relsLabelList.contains(relationship)) {
                        relsLabelList.add(relationship);
                        addToMetaData(ms, relationship, o, 2);
                    }

                    if (DEBUG_PRINT)
                        System.out.println("RELATIONSHIP::  ID(LEFT): " + idL + "\tREL: " +
                                relationship + "\tID(RIGHT): " + idR + "\tAdd Props: " + jsonProps);

                    if (firstLineRels) {
                        returnSchema.put("edges", o.toString());
                        firstLineRels = false;
                    } else {
                        String currentV = returnSchema.get("edges");
                        returnSchema.put("edges", currentV + ", " + o.toString());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (DEBUG_PRINT) {
            System.out.println("NODES : " + returnSchema.get("nodes"));
            System.out.println("EDGES : " + returnSchema.get("edges"));
        }

        ms.createFile();
        return returnSchema;
    }

    private static void addToMetaData(Metadata_Schema ms, String nodeLabel, JsonObject o, int typeAdd) {
        Set<Map.Entry<String, JsonElement>> entries = o.entrySet();

        // all nodes have an internal id
        String metaEntry = "";
        if (typeAdd == 1)
            metaEntry = "id, ";

        for (Map.Entry<String, JsonElement> entry : entries) {
            String keyName = entry.getKey();
            metaEntry += keyName + ", ";
        }

        metaEntry = metaEntry.substring(0, metaEntry.length() - 2);
        ms.addLabelProps(nodeLabel, metaEntry);
    }

    private static Map<String, String> methodOneSchema(String file, boolean DEBUG_PRINT) {
        // read through all lines in the text file containing the schema of the Cypher DB
        Map<String, String> returnSchema = new HashMap<String, String>();
        ArrayList<String> tableNames = new ArrayList<>();

        // regex for deciding whether a line is a node or a relationship
        String patternForNode = "(_\\d+:.*)";
        Pattern patternN = Pattern.compile(patternForNode);

        // regex for deciding whether relationship has properties
        String patternForRel = "\\{.+\\}";
        Pattern patternR = Pattern.compile(patternForRel);

        Matcher m;

        // JSON Parser for creating JSON objects from the text file.
        JsonParser parser = new JsonParser();


        try {
            for (String line : Files.readAllLines(Paths.get(file))) {
                // remove CREATE characters
                line = line.substring(7);

                //using regex to decide between node create or relationship create
                m = patternN.matcher(line);

                // is a node
                if (m.find()) {
                    // firstSplit[0] contains id and entity name
                    // firstSplit[1] contains properties of the entity
                    String[] firstSplit = line.split("` ");

                    String[] idAndTable = firstSplit[0].split(":");
                    String id = idAndTable[0].substring(2);
                    String tableName = idAndTable[1].substring(1, idAndTable[1].length());

                    // remove bad characters from text file
                    String props = firstSplit[1].replace("`", "");

                    JsonObject o = parser.parse(props.substring(0, props.length() - 1)).getAsJsonObject();

                    if (DEBUG_PRINT)
                        System.out.println("NODE::  ID: " + id + "\tTABLE: " + tableName + "\tPROPS: " + o.toString());

                    o.addProperty("id", Integer.parseInt(id));

                    if (returnSchema.containsKey(tableName)) {
                        String currentV = returnSchema.get(tableName);
                        returnSchema.put(tableName, currentV + ", " + o.toString());
                    } else {
                        tableNames.add(tableName);
                        returnSchema.put(tableName, o.toString());
                    }
                } else {
                    // relationship to add to SQL
                    line = line.replace("`", "");

                    //items[0] is left part of relationship
                    //items[1] is relationship identifier
                    //items[2] is the right part (has direction in example but ignoring currently)
                    String[] items = line.split("-");

                    String idL = items[0].substring(2, items[0].length() - 1);
                    String idR = items[2].substring(3, items[2].length() - 1);

                    JsonObject o = new JsonObject();
                    o.addProperty("idL", Integer.parseInt(idL));
                    o.addProperty("idR", Integer.parseInt(idR));

                    String relationship = items[1].substring(2, items[1].length() - 1);

                    //using regex to decide between node create or relationship create
                    m = patternR.matcher(line);

                    String jsonProps = null;

                    if (m.find()) {
                        String[] relAndProps = relationship.split(" \\{");
                        relationship = relAndProps[0];
                        relAndProps[1] = "{".concat(relAndProps[1]);
                        jsonProps = relAndProps[1];
                        o = addToExisitingJSON(o, jsonProps, parser, null);
                    }

                    if (DEBUG_PRINT)
                        System.out.println("RELATIONSHIP::  ID(LEFT): " + idL + "\tREL: " +
                                relationship + "\tID(RIGHT): " + idR + "\tAdd Props: " + jsonProps);

                    if (returnSchema.containsKey(relationship)) {
                        String currentV = returnSchema.get(relationship);
                        returnSchema.put(relationship, currentV + ", " + o.toString());
                    } else {
                        tableNames.add(relationship);
                        returnSchema.put(relationship, o.toString());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (DEBUG_PRINT) {
            for (String s : tableNames) {
                System.out.println(s + " : " + returnSchema.get(s));
            }
        }

        return returnSchema;
    }

    private static JsonObject addToExisitingJSON(JsonObject o, String jsonProps, JsonParser parser,
                                                 String relationship) {
        JsonElement element = parser.parse(jsonProps);
        JsonObject obj = element.getAsJsonObject();
        Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();
        for (Map.Entry<String, JsonElement> entry : entries) {
            String keyName = (relationship == null) ? entry.getKey() : relationship + "_" + entry.getKey();
            o.addProperty(keyName, entry.getValue().toString());
        }
        return o;
    }
}
