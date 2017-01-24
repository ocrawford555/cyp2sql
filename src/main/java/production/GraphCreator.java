package production;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Random;

/**
 * Creates an artificial graph for testing purposes.
 * <p>
 * The value numVertices and String type in this class should be set before running the main method.
 * - numVertices is the number of nodes to be included in this graph.
 * - type refers to the type of graph to be constructed, in terms of its density.
 * The class can generate three types of graphs:
 * - sparse
 * - regular
 * - dense
 * These are based on values recovered from a paper online looking at graph density of real world
 * graphs.
 * <p>
 * The graph constructed is based on a domain of webpages and programmers. Webpages are linked to
 * one another randomly. Programmers can code for zero or more webpages, and these webpages are
 * owned by another set of people known as owners. These owners also employ programmers, although
 * they may employ programmers to code for websites which the owners do not own (in this case the
 * programmer might be a contractor for example). The final relationship in the graph is that
 * programmers and owners can be friends with one another. Note: a person may be both a programmer
 * and an owner.
 * <p>
 * The array allocations assigns a proportion of the nodes to each label.
 * <p>
 * The edges are firstly added to the graph randomly. After the number of edges is greater than the
 * number of nodes, the edges are added proportionally to the degree of each node. Thus, if one node
 * has a larger number of edges already attached to it, it is more likely to have another edge attach
 * to it. These creates slightly more realistic graphs, and helps demonstrate in some way the small
 * world property that a lot of real world graphs exhibit.
 * <p>
 * Features of the graph:
 * - no loops allowed
 * - type of relationship of the edge is based on the labels of the nodes. In most cases this is
 * deterministic (i.e. if the label of the node where the edge is coming from is 'programmer', and
 * the label of the other node is 'website', then this is a "CODES-FOR" relation.)
 * - if no type of relationship can be found for two nodes, then algorithm flips the edge and attempts
 * again. This should add all edges to the graph.
 * <p>
 * The output of the module are CSV files for both the labels and the relationships. These can then be
 * filled with additional metadata, and added to Neo4J through the use of the shell and some existing
 * queries which can parse the .csv.
 * <p>
 * load csv with headers from "file:///csv/owner.csv" AS line
 * CREATE (a:Owner {personID:toInt(line.ownerID), surname:line.surname,
 * city:line.city, state: line.state, cars:toInt(line.cars)});
 * <p>
 * load csv with headers from "file:///csv/progOwner.csv" AS line
 * CREATE (a:Owner:Programmer {personID:toInt(line.poID), surname:line.surname,
 * city:line.city, state: line.state, cars:toInt(line.cars), pets:toInt(line.pets)});
 * <p>
 * load csv with headers from "file:///csv/programmer.csv" AS line
 * CREATE (a:Programmer {personID:toInt(line.progID), surname:line.surname,
 * city:line.city, state:line.state, pets:toInt(line.pets)});
 * <p>
 * load csv with headers from "file:///csv/website.csv" AS line
 * CREATE (a:Website {webID:toInt(line.webID), host:line.host, domain:line.domain});
 * <p>
 * load csv with headers from "file:///csv/CODES_FOR.csv" AS line
 * MATCH (a {personID:toInt(line.sourceID)}), (b:Website {webID:toInt(line.destID)})
 * CREATE (a)-[r:CODES_FOR {commits:toInt(line.commits)}]->(b);
 * <p>
 * load csv with headers from "file:///csv/EMPLOYS.csv" AS line
 * MATCH (a {personID:toInt(line.sourceID)}), (b {personID:toInt(line.destID)})
 * CREATE (a)-[r:EMPLOYS {salary:toInt(line.salary), carShare:line.carShare}]->(b);
 * <p>
 * load csv with headers from "file:///csv/FRIENDS.csv" AS line
 * MATCH (a {personID:toInt(line.sourceID)}), (b {personID:toInt(line.destID)}) CREATE (a)-[r:FRIENDS]->(b);
 * <p>
 * load csv with headers from "file:///csv/LINKED_TO.csv" AS line
 * MATCH (a:Website {webID:toInt(line.sourceID)}), (b:Website {webID:toInt(line.destID)})
 * CREATE (a)-[r:LINKED_TO {popularity:toInt(line.popularity)}]->(b);
 * <p>
 * load csv with headers from "file:///csv/OWNS.csv" AS line
 * MATCH (a {personID:toInt(line.sourceID)}), (b:Website {webID:toInt(line.destID)})
 * CREATE (a)-[r:OWNS {salary:toInt(line.salary)}]->(b);
 */
public class GraphCreator {
    // set these before running the main method.
    private static final int numVertices = 2000;
    // options are SPARSE, REGULAR, DENSE
    private static final String type = "DENSE";

    // density calculation based on d = m / n.
    // where m is the number of edges, and n is the number of nodes.
    private static final double SPARSE_DEGREE = 7.61;
    private static final double REGULAR_DEGREE = 13.49;
    private static final double DENSE_DEGREE = 25.67;
    private static final Random r = new Random();
    // labels of the nodes in the graph.
    private static String[] labels = {"website", "programmer", "owner", "progOwner"};
    // allocations of the number of nodes to the labels above.
    private static int[] allocations = {
            (int) ((0.7) * numVertices),
            (int) ((0.22) * numVertices),
            (int) ((0.05) * numVertices),
            (int) ((0.03) * numVertices)
    };
    private static int numEdges = 0;
    private static long edgesToAdd;
    private static int[] degreeOfVertices;
    private static String[] labelsOfNodes;

    private static int[][] adjMat;

    public static void main(String args[]) {
        try {
            // initialise the parameters and arrays.
            setupAndInitlise();
        } catch (Exception e) {
            System.exit(1);
        }

        // add the edges to the graph.
        addEdges();

        // print basic information about the created graph.
        printInformation();

        // write the output of the graph to a local CSV file for importing into Neo4J.
        try {
            produceCSV();
            produceCSVLabels();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Produce CSV files for the labels - the additional metadata for each label is
     * added manually from other CSV files.
     *
     * @throws FileNotFoundException - File was not found in location specified.
     */
    private static void produceCSVLabels() throws FileNotFoundException {
        StringBuilder web = new StringBuilder();
        StringBuilder prog = new StringBuilder();
        StringBuilder owner = new StringBuilder();
        StringBuilder po = new StringBuilder();

        web.append("webID,host,domain\n");
        prog.append("progID,surname,city,state,pets\n");
        owner.append("ownerID,surname,city,state,cars\n");
        po.append("poID,surname,city,state,pets,cars\n");

        // add the ids of the nodes for each label type to the correct .csv file.
        for (int i = 0; i < labelsOfNodes.length; i++) {
            switch (labelsOfNodes[i]) {
                case "website":
                    web.append(i).append("\n");
                    break;
                case "programmer":
                    prog.append(i).append("\n");
                    break;
                case "owner":
                    owner.append(i).append("\n");
                    break;
                case "progOwner":
                    po.append(i).append("\n");
                    break;
            }
        }

        PrintWriter pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/website.csv"));
        pw.write(web.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/programmer.csv"));
        pw.write(prog.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/owner.csv"));
        pw.write(owner.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/progOwner.csv"));
        pw.write(po.toString());
        pw.close();
    }

    /**
     * Print information of the graph created.
     */
    private static void printInformation() {
        System.out.println("Graph density : " + (double) (numEdges) / (numVertices));
        Arrays.parallelSort(degreeOfVertices);
        System.out.println("Minimal degree of node : " + degreeOfVertices[0]);
        System.out.println("Maximal degree of node : " + degreeOfVertices[numVertices - 1]);
    }

    /**
     * Adds edges to the graph.
     */
    private static void addEdges() {
        // add edges to the graph depending on how sparse/dense the test graph should be
        // the edges are added proportionally to the degree of each node
        // thus, a node with a higher degree than the other nodes has more chance of having
        // edges added to it.
        for (int b = 0; b < edgesToAdd; b++) {
            // add first numVertices edges at random anywhere in the graph.

            if (numEdges < numVertices) {
                int indexFrom = 1;
                int indexTo = 1;
                while (!addEdge(indexFrom, indexTo)) {
                    indexFrom = r.nextInt(numVertices) + 1;
                    indexTo = r.nextInt(numVertices) + 1;
                }
            } else {
                // pick edge based on how edges already been assigned
                // small world principle
                int randomEdgeNumber = r.nextInt(numEdges) + 1;

                // index of node edge leaving from (just for notation, edges have no direction
                // at the moment).
                int indexFrom = 0;

                while (randomEdgeNumber > 0) randomEdgeNumber = randomEdgeNumber - degreeOfVertices[indexFrom++];

                indexFrom--;

                boolean addedEdge = false;

                while (!addedEdge && degreeOfVertices[indexFrom] != numVertices - 1) {
                    int indexTo = r.nextInt(numVertices);

                    if (adjMat[indexFrom][indexTo] != 1 && indexFrom != indexTo) {
                        addedEdge = true;
                        addEdge(indexFrom + 1, indexTo + 1);
                    }
                }
            }
        }
    }

    /**
     * Setup and initialise the graph. This includes:
     * - determining the number of edges needed to produce the correct density value.
     * - assigning labels to nodes
     * - setting all values in the adjacency matrix to 0 (meaning no edge)
     *
     * @throws Exception - parameter 'type' was not one of either: "SPARSE", "REGULAR", or "DENSE".
     */
    private static void setupAndInitlise() throws Exception {
        // setup the number of edges to add to the artificial graph.
        switch (type) {
            case "SPARSE":
                edgesToAdd = (long) (numVertices * SPARSE_DEGREE);
                break;
            case "REGULAR":
                edgesToAdd = (long) (numVertices * REGULAR_DEGREE);
                break;
            case "DENSE":
                edgesToAdd = (long) (numVertices * DENSE_DEGREE);
                break;
            default:
                throw new Exception("Incorrect parameter passed to setup");
        }

        System.out.println(edgesToAdd);

        // setup the adjacency matrix and keep track of the degree of each node.
        degreeOfVertices = new int[numVertices];
        labelsOfNodes = new String[numVertices];
        adjMat = new int[numVertices][numVertices];

        boolean labelAdded;

        // initialise the degree of each node to 0 at the start.
        for (int a = 0; a < numVertices; a++) {
            labelAdded = false;

            // go round loop until the node is assigned a valid label
            while (!labelAdded) {
                int labelAllocation = r.nextInt(labels.length);
                if (allocations[labelAllocation] > 0) {
                    allocations[labelAllocation]--;
                    labelsOfNodes[a] = labels[labelAllocation];
                    labelAdded = true;
                }
            }

            // initialise this array
            degreeOfVertices[a] = 0;

            // NOTE: edges are undirected, direction is decided randomly later on.
            for (int i = 1; i <= numVertices; i++) {
                for (int j = 1; j <= numVertices; j++) {
                    adjMat[i - 1][j - 1] = 0;
                }
            }
        }
    }

    /**
     * Create CSV files for the relationships.
     *
     * @throws FileNotFoundException - file not found in specified location.
     */
    private static void produceCSV() throws FileNotFoundException {
        StringBuilder sb_LINKED_TO = new StringBuilder();
        StringBuilder sb_CODES_FOR = new StringBuilder();
        StringBuilder sb_OWNS = new StringBuilder();
        StringBuilder sb_EMPLOYS = new StringBuilder();
        StringBuilder sb_FRIENDS = new StringBuilder();

        sb_LINKED_TO.append("sourceID,destID,popularity").append("\n");
        sb_CODES_FOR.append("sourceID,destID,commits").append("\n");
        sb_OWNS.append("sourceID,destID,salary").append("\n");
        sb_EMPLOYS.append("sourceID,destID,salary,carShare").append("\n");
        sb_FRIENDS.append("sourceID,destID").append("\n");

        for (int from = 0; from < adjMat.length; from++) {
            for (int to = 0; to < adjMat.length; to++) {
                if (adjMat[from][to] == 1) {
                    if (Math.random() < 0.5) {
                        String labelFrom = labelsOfNodes[from];
                        String labelTo = labelsOfNodes[to];
                        String typeRel = workoutrel(labelFrom, labelTo);

                        if (typeRel != null) {
                            switch (typeRel) {
                                case "LINKED_TO":
                                    sb_LINKED_TO = fillCSV(sb_LINKED_TO, from, to, typeRel);
                                    break;
                                case "CODES_FOR":
                                    sb_CODES_FOR = fillCSV(sb_CODES_FOR, from, to, typeRel);
                                    break;
                                case "OWNS":
                                    sb_OWNS = fillCSV(sb_OWNS, from, to, typeRel);
                                    break;
                                case "EMPLOYS":
                                    sb_EMPLOYS = fillCSV(sb_EMPLOYS, from, to, typeRel);
                                    break;
                                case "FRIENDS":
                                    sb_FRIENDS = fillCSV(sb_FRIENDS, from, to, typeRel);
                                    break;
                            }

                            adjMat[from][to] = 0;
                            adjMat[to][from] = 0;
                        }
                    }
                }
            }
        }

        sb_LINKED_TO = addRemainingEdges(sb_LINKED_TO, "LINKED_TO", 0);
        sb_LINKED_TO = addRemainingEdges(sb_LINKED_TO, "LINKED_TO", 1);
        sb_CODES_FOR = addRemainingEdges(sb_CODES_FOR, "CODES_FOR", 0);
        sb_CODES_FOR = addRemainingEdges(sb_CODES_FOR, "CODES_FOR", 1);
        sb_OWNS = addRemainingEdges(sb_OWNS, "OWNS", 0);
        sb_OWNS = addRemainingEdges(sb_OWNS, "OWNS", 1);
        sb_EMPLOYS = addRemainingEdges(sb_EMPLOYS, "EMPLOYS", 0);
        sb_EMPLOYS = addRemainingEdges(sb_EMPLOYS, "EMPLOYS", 1);
        sb_FRIENDS = addRemainingEdges(sb_FRIENDS, "FRIENDS", 0);
        sb_FRIENDS = addRemainingEdges(sb_FRIENDS, "FRIENDS", 1);

        PrintWriter pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/LINKED_TO.csv"));
        pw.write(sb_LINKED_TO.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/CODES_FOR.csv"));
        pw.write(sb_CODES_FOR.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/OWNS.csv"));
        pw.write(sb_OWNS.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/EMPLOYS.csv"));
        pw.write(sb_EMPLOYS.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/FRIENDS.csv"));
        pw.write(sb_FRIENDS.toString());
        pw.close();

        System.out.println("Done!");
    }

    /**
     * Fill the CSV with the correct data.
     *
     * @param sb      - comma separated string to write for each row.
     * @param from    - index of node where relationship is directed from.
     * @param to      - index of node where relationship is directed to.
     * @param typeRel - the type of relationship.
     * @return - comma separated string with new information in.
     */
    private static StringBuilder fillCSV(StringBuilder sb, int from, int to, String typeRel) {
        sb.append(from).append(",").append(to);

        switch (typeRel) {
            case "LINKED_TO":
                sb.append(",").append(r.nextInt(100));
                break;
            case "CODES_FOR":
                sb.append(",").append(r.nextInt(100));
                break;
            case "OWNS":
                sb.append(",").append((r.nextInt(1000) + 1500) * 2000);
                break;
            case "EMPLOYS":
                String carShare = "no";
                if (Math.random() > 0.5) carShare = "yes";
                sb.append(",").append((r.nextInt(1000) + 1500) * 10).append(",").append(carShare);
                break;
            case "FRIENDS":
                break;
        }
        sb.append("\n");
        return sb;
    }

    /**
     * Add remaining edges to the graph.
     *
     * @param sb  - comma separated string to write for each row.
     * @param rel - the type of relationship to add.
     * @param i   - direction of the relationship - if 1 then inverse from and to indexes.
     * @return comma separated string with new information to write.
     */
    private static StringBuilder addRemainingEdges(StringBuilder sb, String rel, int i) {
        for (int from = 0; from < adjMat.length; from++) {
            for (int to = 0; to < adjMat.length; to++) {
                if (i == 1) {
                    int temp = from;
                    from = to;
                    to = temp;
                }
                if (adjMat[from][to] == 1) {
                    String labelFrom = labelsOfNodes[from];
                    String labelTo = labelsOfNodes[to];
                    String typeRel = workoutrel(labelFrom, labelTo);
                    if (typeRel != null && typeRel.equals(rel)) {
                        sb = fillCSV(sb, from, to, typeRel);
                        adjMat[from][to] = 0;
                        adjMat[to][from] = 0;
                    }
                }
            }
        }
        return sb;
    }

    /**
     * Workout the relationship between the two nodes, based on their labels only.
     *
     * @param labelFrom - label of node where relationship is coming from.
     * @param labelTo   - label of node where relationship is going to.
     * @return String value of the type of relationship.
     */
    private static String workoutrel(String labelFrom, String labelTo) {
        if (labelFrom.equals("website") && labelTo.equals("website")) {
            return "LINKED_TO";
        } else if ((labelFrom.equals("programmer") || labelFrom.equals("progOwner")) && labelTo.equals("website")) {
            if (labelFrom.equals("progOwner")) {
                if (Math.random() > 0.5) return "OWNS";
            }
            return "CODES_FOR";
        } else if (labelFrom.equals("owner") && labelTo.equals("website")) {
            return "OWNS";
        } else if ((labelFrom.equals("owner") || labelFrom.equals("progOwner")) &&
                (labelTo.equals("programmer") || labelTo.equals("progOwner"))) {
            return "EMPLOYS";
        } else if ((labelFrom.equals("programmer") || labelFrom.equals("progOwner")) &&
                (labelTo.equals("programmer") || labelTo.equals("progOwner"))) {
            return "FRIENDS";
        } else if ((labelFrom.equals("owner") || labelFrom.equals("progOwner"))
                && (labelTo.equals("owner") || labelTo.equals("progOwner"))) {
            return "FRIENDS";
        } else {
            return null;
        }
    }

    /**
     * Helped method to output the values in the adjacency matrix.
     * Note : only really for debugging and small matrices.
     *
     * @param adjMat Adjacency matrix to print out.
     */
    private static void printMatirx(int[][] adjMat) {
        int k = 0;
        System.out.print("          ");
        for (int l = 1; l <= adjMat.length; l++) {
            if (l < 9)
                System.out.print(l + "   ");
            else System.out.print(l + "  ");
        }
        System.out.println();
        for (int[] anAdjMat : adjMat) {
            System.out.print(++k + "   :\t ");
            for (int j = 0; j < adjMat.length; j++) {
                System.out.print("[" + (anAdjMat[j] == 1 ? "x" : ((j + 1) == k) ? "\\" : " ") + "] ");
            }
            System.out.println();
        }
    }

    /**
     * Add edge to the adjacency matrix. No loops are allowed in the graph.
     *
     * @param from - index of node where edge coming from.
     * @param to   - index of the node where edge is going to.
     * @return True: edge added successfully. False: if edge already exists, or values 'from' and 'to' are equal.
     */
    private static boolean addEdge(int from, int to) {
        from--;
        to--;

        // edge already there or indexes the same (i.e a loop)
        if ((adjMat[from][to] == 1) || from == to) return false;

        adjMat[from][to] = 1;
        adjMat[to][from] = 1;
        degreeOfVertices[to]++;
        degreeOfVertices[from]++;
        numEdges++;
        return true;
    }
}
