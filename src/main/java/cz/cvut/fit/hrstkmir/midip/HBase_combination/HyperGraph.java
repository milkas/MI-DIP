package cz.cvut.fit.hrstkmir.midip.HBase_combination;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import edu.uci.ics.jung.graph.SetHypergraph;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author mira
 */
public final class HyperGraph {

    File metadata;
    String delims = ";";
    SetHypergraph<String, String> H = new SetHypergraph<>();
    Multimap<String, String> T = HashMultimap.create();

    public HyperGraph() {
    }
    
    public SetHypergraph<String, String> get(){
        return H;
    }

    public SetHypergraph<String, String> createGraph(String file) {
        T = this.createMap(file);

        Set keySet = T.keySet();
        Iterator keyIterator = keySet.iterator();
        List<String> edge;
        while (keyIterator.hasNext()) {
            edge = new ArrayList<>();
            String key = (String) keyIterator.next();
            Collection<String> values = T.get(key);

            for (String value : values) {
                edge.add(value);
                H.addVertex(value);
            }
            H.addEdge(key, edge);
        }

        return H;
    }

    private Multimap<String, String> createMap(String file) {
        this.metadata = LoadFile(file);
        String line;
        int lineNumber = 0;
        try (
                InputStream fis = new FileInputStream(metadata);
                InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
                BufferedReader br = new BufferedReader(isr)) {
            while ((line = br.readLine()) != null) {
                //přeskočit 2 první řádky
                if (lineNumber < 2) {
                    lineNumber++;
                    continue;
                }
                String[] tokens = line.split(delims);
                String[] rcs = tokens[2].split(" ");
                for (String rc : rcs) {
                    T.put(tokens[0], rc.trim());
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(HyperGraph.class.getName()).log(Level.SEVERE, null, ex);
        }
        return T;
    }

    private static File LoadFile(String path) {
        return (new File(path));
    }

}
