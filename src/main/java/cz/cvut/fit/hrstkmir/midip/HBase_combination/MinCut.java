package cz.cvut.fit.hrstkmir.midip.HBase_combination;

import edu.uci.ics.jung.graph.SetHypergraph;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MinCut {

    private MinCut() {

    }

    public static Map<String, String> MinCut(SetHypergraph<String, String> H) {
        Map<String, String> M = new HashMap<>();

        /*vytvoříme nebo přiřadíme soubor, do kterého se uloží reprezentace grafu ve tvaru,
         který vyžaduje program PATOH. */
        try {
            File patoh = new File("patoh_graf.txt");
            if (!patoh.exists()) {
                patoh.createNewFile();
            }
            FileOutputStream oFile = new FileOutputStream(patoh, false);
        } catch (IOException ex) {
            Logger.getLogger(MinCut.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        
        
        
        
        

        Collection<String> S = H.getVertices();

        for (String key : S) {

            M.put(key, "pref" + key);
        }

        return M;
    }

}
