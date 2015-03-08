package cz.cvut.fit.hrstkmir.midip.HBase_combination;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 *
 * @author mira
 */
public class CreateHyperGraph {

    public static void main(String[] args) throws Exception {

        final File metadataFolder = new File("/home/mira/metadata");
        Multimap<String, Long> H = HashMultimap.create();
        Multimap<String, Long> T = HashMultimap.create();

        for (final File job : metadataFolder.listFiles()) {

            String delims = ";";

            if (job.isDirectory()) {

                for (final File chunk : job.listFiles()) {
                    String line;
                    int lineNumber = 0;
                    try (
                            InputStream fis = new FileInputStream(chunk);
                            InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
                            BufferedReader br = new BufferedReader(isr)) {
                        while ((line = br.readLine()) != null) {

                            if (lineNumber < 2) {

                                lineNumber++;

                                continue;
                            }

                            String[] tokens = line.split(delims);

                            String[] rcs = tokens[2].split(delims);

                            for (String rc : rcs) {
                                T.put(tokens[0], Long.parseLong(rc));

                            }

                        }
                    }

                }

            }

        }
        
        
            
        
        
        
        
        
        
//        Set keySet = T.keySet();
//        Iterator keyIterator = keySet.iterator();
//        while (keyIterator.hasNext()) {
//            String key = (String) keyIterator.next();
//            Collection<Long> values = T.get(key);
//            
//
//        }

    }

}
