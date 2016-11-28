package schemaConversion;

import com.google.gson.JsonParser;
import production.c2sqlV1;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

public class SchemaTranslate {
    // storing all the labels for nodes and edges.
    public static List<String> nodeRelLabels = Collections.synchronizedList(new ArrayList<>());
    public static List<String> edgesRelLabels = Collections.synchronizedList(new ArrayList<>());

    // storing separate information for types of nodes
    public static Map<String, String> labelMappings = Collections.synchronizedMap(new HashMap<>());

    // workspace area for both nodes and edges
    public static String nodesFile = c2sqlV1.workspaceArea + "/nodes.txt";
    public static String edgesFile = c2sqlV1.workspaceArea + "/edges.txt";

    // JSON Parser for creating JSON objects from the text file.
    // passed to all of the threads
    static JsonParser parser = new JsonParser();

    // regex for deciding whether a line is a node or a relationship
    private static String patternForNode = "(_\\d+:.*)";
    static Pattern patternN = Pattern.compile(patternForNode);

    // regex for deciding whether relationship has properties
    private static String patternForRel = "\\{.+\\}";
    static Pattern patternR = Pattern.compile(patternForRel);

    /**
     * Main method for translating the schema.
     *
     * @param file Dump File from Neo4J.
     */
    public static void translate(String file) {
        // perform initial preprocess of the file to remove content such as new file markers
        // and other aspects that will break the schema converter.
        // return number of lines if no issue, else -1
        int count = performPreProcessFile(file);
        if (count == -1) return;

        // number of concurrent threads to work on dump file. Currently 4. Test.
        final int segments = 4;
        final int amountPerSeg = (int) Math.ceil(count / segments);

        ArrayList<String> s1 = new ArrayList<>();
        ArrayList<String> s2 = new ArrayList<>();
        ArrayList<String> s3 = new ArrayList<>();
        ArrayList<String> s4 = new ArrayList<>();

        try {
            // open correctly preparsed file
            FileInputStream fis = new FileInputStream(file.replace(".txt", "_new.txt"));

            //Construct BufferedReader from InputStreamReader
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;

            int segNum = 0;
            int amountInSeg = 0;

            while ((line = br.readLine()) != null) {
                if (amountInSeg++ <= amountPerSeg) {
                    switch (segNum) {
                        case 0:
                            s1.add(line);
                            break;
                        case 1:
                            s2.add(line);
                            break;
                        case 2:
                            s3.add(line);
                            break;
                        case 3:
                            s4.add(line);
                            break;
                    }
                } else {
                    segNum++;
                    switch (segNum) {
                        case 0:
                            s1.add(line);
                            break;
                        case 1:
                            s2.add(line);
                            break;
                        case 2:
                            s3.add(line);
                            break;
                        case 3:
                            s4.add(line);
                            break;
                    }
                    amountInSeg = 1;
                }
            }

            // file indicators for the four threads to output to.
            String[] files = {"a", "b", "c", "d"};

            Thread[] ts = new Thread[segments];
            for (int i = 0; i < ts.length; i++) {
                ArrayList<String> temp = null;
                switch (i) {
                    case 0:
                        temp = s1;
                        break;
                    case 1:
                        temp = s2;
                        break;
                    case 2:
                        temp = s3;
                        break;
                    case 3:
                        temp = s4;
                        break;
                }
                ts[i] = new Thread(new PerformWork(temp, files[i]));
            }

            for (Thread q : ts) {
                q.start();
            }

            int done = 0;
            while (done < ts.length) {
                try {
                    ts[done].join();
                    done++;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            combineWork(files);

            // remove strange duplicates appearing in the ArrayList
            Set<String> hs = new HashSet<>();
            hs.addAll(nodeRelLabels);
            nodeRelLabels.clear();
            nodeRelLabels.addAll(hs);
            hs.clear();

            hs.addAll(edgesRelLabels);
            edgesRelLabels.clear();
            edgesRelLabels.addAll(hs);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Concatenate result of individual threads to one file. One method call does this
     * for both the nodes and relationships.
     *
     * @param files 4 files resulted from reading initial dump.
     * @throws IOException
     */
    private static void combineWork(String[] files) throws IOException {
        OutputStream out = null;
        byte[] buf;

        for (int i = 0; i < 2; i++) {
            String file = (i == 0) ? nodesFile : edgesFile;
            out = new FileOutputStream(file);
            buf = new byte[1024];
            for (String a : files) {
                InputStream in = new FileInputStream(file.replace(".txt", a + ".txt"));
                int b;
                while ((b = in.read(buf)) >= 0) {
                    out.write(buf, 0, b);
                    out.flush();
                }
                in.close();
                File f = new File(file.replace(".txt", a + ".txt"));
                f.delete();
            }
        }

        out.close();
    }

    /**
     * Perform an initial scan of the file and remove invalid new line characters.
     *
     * @param file
     * @return New file
     */
    private static int performPreProcessFile(String file) {
        FileInputStream fis;
        FileOutputStream fos;
        int count = 0;

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
                count++;

                // escape character in SQL (' replaced with '')
                line = line.replace("'", "''");

                if (line.startsWith("create")) {
                    if (firstLine) {
                        firstLine = false;
                    } else {
                        bw.write(output);
                        bw.newLine();
                    }
                    output = line;
                } else if (line.isEmpty()) {
                    // do nothing intentionally
                } else {
                    output += line;
                }
            }

            br.close();
            bw.write(output);
            bw.flush();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
        return count;
    }

}
