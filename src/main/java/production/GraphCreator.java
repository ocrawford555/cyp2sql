package production;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Random;

public class GraphCreator {
    private static final int numVertices = 500;
    private static final double SPARSE_DEGREE = 7.61;
    private static final double REGULAR_DEGREE = 13.49;
    private static final double DENSE_DEGREE = 25.67;

    private static final Random r = new Random();

    private static int numEdges = 0;
    private static long edgesToAdd;
    private static int[] degreeOfVertices;
    private static String[] labelsOfNodes;
    private static int[][] adjMat;
    private static String[] labels = {"website", "programmer", "owner", "progOwner"};

    private static int[] allocations = {
            (int) ((0.7) * numVertices),
            (int) ((0.22) * numVertices),
            (int) ((0.05) * numVertices),
            (int) ((0.03) * numVertices)
    };

    public static void main(String args[]) {
        String type = "SPARSE";
        try {
            setupAndInitlise(type);
        } catch (Exception e) {
            System.exit(1);
        }

        addEdges();

        printInformation();

        // write the output of the graph to a local CSV file for importing into Neo4J.
        try {
            produceCSV(type);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        try {
            produceCSVLabels(type);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void produceCSVLabels(String type) throws FileNotFoundException {
        StringBuilder web = new StringBuilder();
        StringBuilder prog = new StringBuilder();
        StringBuilder owner = new StringBuilder();
        StringBuilder po = new StringBuilder();

        web.append("webID\n");
        prog.append("progID\n");
        owner.append("ownerID\n");
        po.append("poID\n");

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

        PrintWriter pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/website-" + type + ".csv"));
        pw.write(web.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/programmer-" + type + ".csv"));
        pw.write(prog.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/owner-" + type + ".csv"));
        pw.write(owner.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/progOwner-" + type + ".csv"));
        pw.write(po.toString());
        pw.close();

    }

    private static void printInformation() {
        // print out the node degree distribution and density
        System.out.println(Arrays.toString(degreeOfVertices));
        System.out.println("Graph density : " + (double) (numEdges) / (numVertices));
        Arrays.parallelSort(degreeOfVertices);
        System.out.println("Minimal degree of node : " + degreeOfVertices[0]);
        System.out.println("Maximal degree of node : " + degreeOfVertices[numVertices - 1]);
    }

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

    private static void setupAndInitlise(String sparse) throws Exception {
        // setup the number of edges to add to the artificial graph.
        switch (sparse) {
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

    private static void produceCSV(String type) throws FileNotFoundException {
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
                                    sb_LINKED_TO = fillCSV(sb_LINKED_TO, from, to, typeRel, r);
                                    break;
                                case "CODES_FOR":
                                    sb_CODES_FOR = fillCSV(sb_CODES_FOR, from, to, typeRel, r);
                                    break;
                                case "OWNS":
                                    sb_OWNS = fillCSV(sb_OWNS, from, to, typeRel, r);
                                    break;
                                case "EMPLOYS":
                                    sb_EMPLOYS = fillCSV(sb_EMPLOYS, from, to, typeRel, r);
                                    break;
                                case "FRIENDS":
                                    sb_FRIENDS = fillCSV(sb_FRIENDS, from, to, typeRel, r);
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

        PrintWriter pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/LINKED_TO-" + type + ".csv"));
        pw.write(sb_LINKED_TO.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/CODES_FOR-" + type + ".csv"));
        pw.write(sb_CODES_FOR.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/OWNS-" + type + ".csv"));
        pw.write(sb_OWNS.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/EMPLOYS-" + type + ".csv"));
        pw.write(sb_EMPLOYS.toString());
        pw.close();
        pw = new PrintWriter(new File("C:/Users/ocraw/Desktop/FRIENDS-" + type + ".csv"));
        pw.write(sb_FRIENDS.toString());
        pw.close();

        System.out.println("done!");
    }

    private static StringBuilder fillCSV(StringBuilder sb, int from, int to, String typeRel, Random r) {
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
                        sb = fillCSV(sb, from, to, typeRel, r);
                        adjMat[from][to] = 0;
                        adjMat[to][from] = 0;
                    }
                }
            }
        }
        return sb;
    }

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
