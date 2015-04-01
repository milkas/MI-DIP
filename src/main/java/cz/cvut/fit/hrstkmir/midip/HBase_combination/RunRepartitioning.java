
package cz.cvut.fit.hrstkmir.midip.HBase_combination;

import java.io.IOException;


/**
 *
 * @author mira
 */
public class RunRepartitioning {
    public static void main(String[] args) throws IOException {
        String originalDB = "lide2";
        String newDB = "lide3";
        
        HyperGraph H = new HyperGraph();
        H.createGraph("/home/mira/metadata/job/metadata.txt");
        Repartitioning.Repartitioning(MinCut.MinCut(H.get()), originalDB, newDB);
    
    
    }
    
}
