package cz.cvut.fit.hrstkmir.midip.HBase_combination;

import cz.cvut.fit.hrstkmir.midip.tools.ExecuteShellComand;
import edu.uci.ics.jung.graph.SetHypergraph;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MinCut {

    private static int nodes;
    private static int edges;
    private static int pins;
    private static final int indexBase = 0;
    static ArrayList<String> nodesArray = new ArrayList<>();
    static ArrayList<String> edgesArray = new ArrayList<>();
    static SetHypergraph<String, String> hypergraph;
    static PrintWriter writer;

    private MinCut() {

    }

    public static Map<String, Integer> MinCut(SetHypergraph<String, String> H, int numberOfPartitions) {
        hypergraph = H;

        createFile();
        mapNodes();
        mapEdges();
        countPins();

        //write header of PATOH file;
        writer.print(indexBase);
        writer.print(" ");
        writer.print(nodes);
        writer.print(" ");
        writer.print(edges);
        writer.print(" ");
        writer.println(pins);

        //fill file with nodes and edges
        for (int i = 0; i < edgesArray.size(); i++) {

            Collection<String> S = hypergraph.getIncidentVertices(edgesArray.get(i));

            for (String key : S) {
                writer.print(nodesArray.indexOf(key));
                writer.print(" ");
            }
            writer.println();
        }

        writer.close();

        //run Patoh program
        ExecuteShellComand.ExecutePATOHProgram(numberOfPartitions);

        Map<String, Integer> partitionedGraph = new HashMap<>();

        String line;
        int idInMap = 0;
        try (
                InputStream fis = new FileInputStream(new File("./patohGraph.txt.part.4"));
                InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
                BufferedReader br = new BufferedReader(isr)) {
            while ((line = br.readLine()) != null) {

                String[] tokens = line.split(" ");
                for (String token : tokens) {
                    partitionedGraph.put(nodesArray.get(idInMap), Integer.parseInt(token));

                    idInMap++;
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(HyperGraph.class.getName()).log(Level.SEVERE, null, ex);
        }


        return partitionedGraph;

    }

    private static void mapNodes() {
        nodes = 0;
        Collection<String> S = hypergraph.getVertices();

        for (String key : S) {
            nodesArray.add(nodes, key);
            nodes++;
        }
    }

    private static void mapEdges() {
        edges = 0;
        Collection<String> S = hypergraph.getEdges();

        for (String key : S) {
            edgesArray.add(edges, key);
            edges++;
        }
    }

    private static void countPins() {
        pins = 0;
        Collection<String> S = hypergraph.getVertices();

        for (String key : S) {
            pins = pins + hypergraph.degree(key);
        }
    }

    private static void createFile() {
        try {
            writer = new PrintWriter("patohGraph.txt", "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException ex) {
            Logger.getLogger(MinCut.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
