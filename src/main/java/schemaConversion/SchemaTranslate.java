package schemaConversion;

import com.google.gson.JsonParser;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class SchemaTranslate {
    // JSON Parser for creating JSON objects from the text file.
    // passed to all of the threads
    static JsonParser parser = new JsonParser();
    static String nodesFile = "C:/Users/ocraw/Desktop/nodes.txt";
    static String edgesFile = "C:/Users/ocraw/Desktop/edges.txt";
    // regex for deciding whether a line is a node or a relationship
    private static String patternForNode = "(_\\d+:.*)";
    static Pattern patternN = Pattern.compile(patternForNode);
    // regex for deciding whether relationship has properties
    private static String patternForRel = "\\{.+\\}";
    static Pattern patternR = Pattern.compile(patternForRel);

    public static void translate(String file, boolean DEBUG_PRINT) {
        // perform initial preprocess of the file to remove content such as new file markers
        // and other aspects that will break the schema converter.
        // returns true if no issue, else fails
        int count = performPreProcessFile(file);
        if (count == -1) return;


        // create a class for the generation of the additional metadata file
        // TODO: implement the meta file, leave alone for now whilst working out concurrent thing.
        // Metadata_Schema ms = new Metadata_Schema();

        int lines = count;
        final int segments = 4;
        final int amountPerSeg = (int) Math.ceil(lines / segments);

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
                ts[i] = new Thread(new PerformWork(temp, i + 1, files[i]));
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

            OutputStream out = new FileOutputStream(nodesFile);
            byte[] buf = new byte[1024];
            for (String a : files) {
                InputStream in = new FileInputStream(nodesFile.replace(".txt", a + ".txt"));
                int b;
                while ((b = in.read(buf)) >= 0) {
                    out.write(buf, 0, b);
                    out.flush();
                }
                in.close();
                File f = new File(nodesFile.replace(".txt", a + ".txt"));
                f.delete();
            }
            out.close();

            out = new FileOutputStream(edgesFile);
            buf = new byte[1024];
            for (String a : files) {
                InputStream in = new FileInputStream(edgesFile.replace(".txt", a + ".txt"));
                int b;
                while ((b = in.read(buf)) >= 0) {
                    out.write(buf, 0, b);
                    out.flush();
                }
                in.close();
                File f = new File(edgesFile.replace(".txt", a + ".txt"));
                f.delete();
            }
            out.close();


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

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
                    //do nothing
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
