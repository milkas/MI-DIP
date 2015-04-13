package cz.cvut.fit.hrstkmir.midip.HBase_combination;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author mira
 */
public final class Repartitioning {

    private Repartitioning() {

    }

    public static void Repartitioning(Map<String, Integer> M, String originalDB, String newDB) throws IOException {

        Configuration conf = HBaseConfiguration.create();

        HTable originalTable = new HTable(conf, originalDB);
        HTable newTable = new HTable(conf, newDB);

        HBaseAdmin hbase = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(newDB);
        HColumnDescriptor meta = new HColumnDescriptor("osoba".getBytes());
        HColumnDescriptor prefix = new HColumnDescriptor("sluzba".getBytes());
        desc.setValue(HTableDescriptor.SPLIT_POLICY, KeyPrefixRegionSplitPolicy.class.getName());
        desc.setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY, String.valueOf(1));
        desc.addFamily(meta);
        desc.addFamily(prefix);
        hbase.createTable(desc);

        for (Map.Entry<String, Integer> entry : M.entrySet()) {
            System.out.println(entry.getKey() + "/" + entry.getValue());
            String key = entry.getValue() + entry.getKey();
            System.out.println(key);
            Put put = new Put(Bytes.toBytes(key));

            Get get = new Get(Bytes.toBytes(entry.getKey()));
            Result result = originalTable.get(get);
//            for (int i = 0;result.size()< i; i++){
//                
//                result.getValue(Bytes.toBytes("osoba"), Bytes.toBytes("kraj"));
//                put.add(Bytes.toBytes("osoba"), Bytes.toBytes("jmeno"), Bytes.toBytes(person.getName()));
//            }

            byte[] value;
            value = result.getValue(Bytes.toBytes("osoba"), Bytes.toBytes("jmeno"));
            put.add(Bytes.toBytes("osoba"), Bytes.toBytes("jmeno"), value);
            value = result.getValue(Bytes.toBytes("osoba"), Bytes.toBytes("prijmeni"));
            put.add(Bytes.toBytes("osoba"), Bytes.toBytes("prijmeni"), value);
            value = result.getValue(Bytes.toBytes("osoba"), Bytes.toBytes("pohlavi"));
            put.add(Bytes.toBytes("osoba"), Bytes.toBytes("pohlavi"), value);
            value = result.getValue(Bytes.toBytes("osoba"), Bytes.toBytes("obec"));
            put.add(Bytes.toBytes("osoba"), Bytes.toBytes("obec"), value);
            value = result.getValue(Bytes.toBytes("osoba"), Bytes.toBytes("okres"));
            put.add(Bytes.toBytes("osoba"), Bytes.toBytes("okres"), value);
            value = result.getValue(Bytes.toBytes("osoba"), Bytes.toBytes("kraj"));
            put.add(Bytes.toBytes("osoba"), Bytes.toBytes("kraj"), value);
           
             newTable.put(put);
        }

    }

}
