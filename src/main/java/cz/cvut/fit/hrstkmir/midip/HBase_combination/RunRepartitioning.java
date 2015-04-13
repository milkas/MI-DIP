
package cz.cvut.fit.hrstkmir.midip.HBase_combination;

import java.io.IOException;


/**
 *
 * @author mira
 */
public class RunRepartitioning {
    public static void main(String[] args) throws IOException {
        String originalDB = "lide-sluzby2";
        String newDB = "lide-sluzby-new";
        int numberOfPartitions = 4;
        
        HyperGraph H = new HyperGraph();
        H.createGraph("/home/mira/metadata/job/metadata.txt");
        Repartitioning.Repartitioning(MinCut.MinCut(H.get(),numberOfPartitions), originalDB, newDB);
    
    }
    
}
