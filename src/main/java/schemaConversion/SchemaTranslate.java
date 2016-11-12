package schemaConversion;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaTranslate {
    public static Map<String, String> translate(String file, boolean DEBUG_PRINT) {
        // perform initial preprocess of the file to remove content such as new file markers
        // and other aspects that will break the schema converter.
        boolean success = performPreProcessFile(file);

        if (!success) return null;

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
            FileInputStream fis = new FileInputStream(file.replace(".txt", "_new.txt"));

            //Construct BufferedReader from InputStreamReader
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;

            while ((line = br.readLine()) != null) {
                // remove CREATE characters
                line = line.substring(7).toLowerCase();

                //using regex to decide between node or relationship
                m = patternN.matcher(line);

                // is a node
                if (m.find()) {
                    // firstSplit[0] contains id and node label
                    // firstSplit[1] contains properties of the node
                    String[] firstSplit = line.split("` ");

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

                    JsonObject o = (JsonObject) parser.parse(props.substring(0, props.length() - 1));

                    // add to the metadata file
                    if (!nodeLabelList.contains(nodeLabel)) {
                        nodeLabelList.add(nodeLabel);
                        // 1 for nodes
                        addToMetaData(ms, nodeLabel, o, 1);
                    }

                    if (DEBUG_PRINT)
                        System.out.println("NODE::  ID: " + id + "\tLABEL: " + nodeLabel + "\tPROPS: " + o.toString());

                    o.addProperty("id", id);
                    o.addProperty("label", nodeLabel);

                    //TODO: could be made more efficient
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
                    String[] items = line.split("\\)-");

                    int idL = Integer.parseInt(items[0].substring(2, items[0].length()));

                    String[] innerItems = items[1].split("->");
                    int idR = Integer.parseInt(innerItems[1].substring(2, innerItems[1].length() - 1));

                    JsonObject o = new JsonObject();
                    o.addProperty("idL", idL);
                    o.addProperty("idR", idR);

                    String relationship = innerItems[0].substring(2, innerItems[0].length() - 1);

                    // does the relationship have properties
                    m = patternR.matcher(line);
                    String jsonProps = null;

                    if (m.find()) {
                        String[] relAndProps = relationship.split(" \\{");
                        relationship = relAndProps[0];
                        relAndProps[1] = "{".concat(relAndProps[1]);
                        jsonProps = relAndProps[1];
                        o = addToExisitingJSON(o, jsonProps, parser);
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
            //System.out.println("EDGES : " + returnSchema.get("edges"));
        }

        ms.createFile();
        return returnSchema;
    }

    private static boolean performPreProcessFile(String file) {
        FileInputStream fis;
        FileOutputStream fos;

        try {
            fis = new FileInputStream(file);
            //Construct BufferedReader from InputStreamReader
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;

            fos = new FileOutputStream(file.replace(".txt", "_new.txt"));

            //Construct BufferedReader from InputStreamReader
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            String output = "";
            boolean firstLine = true;


            while ((line = br.readLine()) != null) {
                if (line.startsWith("create")) {
                    if (firstLine) {
                        firstLine = false;
                    } else {
                        bw.write(output);
                        bw.newLine();
                    }
                    output = line;
                } else if (line.isEmpty()) {
                    //do nothing
                } else {
                    output += line;
                }
            }

            br.close();
            bw.flush();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
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

    private static JsonObject addToExisitingJSON(JsonObject o, String jsonProps, JsonParser parser) {
        JsonElement element = parser.parse(jsonProps);
        JsonObject obj = element.getAsJsonObject();
        Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();
        for (Map.Entry<String, JsonElement> entry : entries) {
            // TODO: having to remove comma to make work, fix this issue in later versions
            // possibly involve changing JSON library
            o.addProperty(entry.getKey(), String.valueOf(entry.getValue()).replace("\"", ""));
        }
        return o;
    }
}
