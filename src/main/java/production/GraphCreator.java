package production;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Random;

public class GraphCreator {
    private static final int numVertices = 100;
    private static int numEdges = 0;
    private static int[] degreeOfVertices;
    private static String[] labelsOfNodes;
    private static int[][] adjMat;
    private static String[] labels = {"website", "programmer", "owner"};
    private static int[] allocations = {
            (int) ((0.8) * numVertices),
            (int) ((0.15) * numVertices),
            (int) ((0.05) * numVertices)
    };

    public static void main(String args[]) {
        // setup the adjacency matrix and keep track of the degree of each node.
        degreeOfVertices = new int[numVertices];
        labelsOfNodes = new String[numVertices];
        adjMat = new int[numVertices][numVertices];

        // setup a Random object to help build the graph.
        Random r = new Random();

        boolean labelAdded;
        // initialise the degree of each node to 0 at the start.
        for (int a = 0; a < numVertices; a++) {
            labelAdded = false;
            while (!labelAdded) {
                int labelAllocation = r.nextInt(labels.length);
                if (allocations[labelAllocation] > 0) {
                    allocations[labelAllocation]--;
                    labelsOfNodes[a] = labels[labelAllocation];
                    labelAdded = true;
                }
            }
            degreeOfVertices[a] = 0;
        }

        // adjacent indexes connected in the graph. (1 --> 2, 2 --> 3, ..., 999 --> 1000).
        // NOTE: edges are undirected, direction is decided randomly later on.
        for (int i = 1; i <= numVertices; i++) {
            for (int j = 1; j <= numVertices; j++) {
                if (i - j == 1) {
                    addEdge(i, j);
                } else adjMat[i - 1][j - 1] = 0;
            }
        }

        // add edges to the graph depending on how sparse/dense the test graph should be
        // the edges are added proportionally to the degree of each node
        // thus, a node with a higher degree than the other nodes has more chance of having
        // edges added to it.
        for (int b = 0; b < 86825; b++) {
            int v = r.nextInt(numEdges) + 1;
            int indexFrom = 0;

            boolean broken = false;

            while (v > 0) {
                if (indexFrom == numVertices) {
                    broken = true;
                    break;
                }
                v = v - degreeOfVertices[indexFrom++];
            }

            if (broken) break;

            indexFrom--;
            boolean addedEdge = false;

            while (!addedEdge && degreeOfVertices[indexFrom] != numVertices - 1) {
                int possTo = r.nextInt(numVertices);

                if (adjMat[indexFrom][possTo] != 1 && indexFrom != possTo) {
                    addedEdge = true;
                    addEdge(indexFrom + 1, possTo + 1);
                }
            }
        }

        // print out the node degree distribution and density
        System.out.println(Arrays.toString(degreeOfVertices));
        System.out.println("Graph density : " + (double) (numEdges) / ((numVertices) * (numVertices - 1)));
        Arrays.parallelSort(degreeOfVertices);
        System.out.println("Minimal degree of node : " + degreeOfVertices[0]);
        System.out.println("Maximal degree of node : " + degreeOfVertices[numVertices - 1]);

        // write the output of the graph to a local CSV file for importing into Neo4J.
        try {
            produceCSV(r);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    private static void produceCSV(Random r) throws FileNotFoundException {
        PrintWriter pw = new PrintWriter(new File("D:/dense500.csv"));
        StringBuilder sb = new StringBuilder();
        sb.append("sourceID,destID,popularity,commits,salary,carShare").append("\n");
        for (int from = 0; from < adjMat.length; from++) {
            for (int to = 0; to < adjMat.length; to++) {
                if (adjMat[from][to] == 1) {
                    if (Math.random() < 0.5) {
                        String labelFrom = labelsOfNodes[from];
                        String labelTo = labelsOfNodes[to];
                        String typeRel = workoutrel(labelFrom, labelTo);
                        if (typeRel != null) {
                            sb = fillCSV(sb, from, to, typeRel, r);
                            adjMat[from][to] = 0;
                            adjMat[to][from] = 0;
                        }
                    }
                }
            }
        }

        sb = addRemainingEdges(sb, 0, r);
        sb = addRemainingEdges(sb, 1, r);

        pw.write(sb.toString());
        pw.close();
        System.out.println("done!");
    }

    private static StringBuilder fillCSV(StringBuilder sb, int from, int to, String typeRel, Random r) {
        sb.append(from).append(",").append(to).append(",");

        switch (typeRel) {
            case "LINKED_TO":
                sb.append(r.nextInt(1000)).append(0).append(0).append(0).append("\n");
                break;
            case "CODES_FOR":
                sb.append(0).append(r.nextInt(1000)).append(0).append(0).append("\n");
                break;
            case "OWNS":
                sb.append(0).append(0).append((r.nextInt(1000) + 1500) * 2000).append(0).append("\n");
                break;
            case "EMPLOYS":
                String carShare = "no";
                if (Math.random() > 0.5) carShare = "yes";
                sb.append(0).append(0).append((r.nextInt(1000) + 1500) * 100).append(carShare).append("\n");
                break;
            case "FRIENDS":
                sb.append(0).append(0).append(0).append(0).append("\n");
                break;
        }
        return sb;
    }

    private static StringBuilder addRemainingEdges(StringBuilder sb, int i, Random r) {
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
                    if (typeRel != null) {
                        sb.append(from).append(",").append(to).append(",").append(r.nextInt(1000)).append("\n");
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
        } else if (labelFrom.equals("programmer") && labelTo.equals("website")) {
            return "CODES_FOR";
        } else if (labelFrom.equals("owner") && labelTo.equals("website")) {
            return "OWNS";
        } else if (labelFrom.equals("owner") && labelTo.equals("programmer")) {
            return "EMPLOYS";
        } else if (labelFrom.equals("programmer") && labelTo.equals("programmer")) {
            return "FRIENDS";
        } else if (labelFrom.equals("owner") && labelTo.equals("owner")) {
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

    private static void addEdge(int from, int to) {
        from--;
        to--;
        adjMat[from][to] = 1;
        adjMat[to][from] = 1;
        degreeOfVertices[to]++;
        degreeOfVertices[from]++;
        numEdges += 2;
    }
}
