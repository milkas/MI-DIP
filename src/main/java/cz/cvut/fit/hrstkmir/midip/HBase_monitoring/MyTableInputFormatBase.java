/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.cvut.fit.hrstkmir.midip.HBase_monitoring;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
;

public class MyTableInputFormatBase extends TableInputFormat {

@Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
       InputSplit split, TaskAttemptContext context)
   throws IOException {
      
    TableSplit tSplit = (TableSplit) split;
   TableRecordReader trr = new MyTableRecordReader();
    
    Scan sc = new Scan(this.getScan());
    sc.setStartRow(tSplit.getStartRow());
    sc.setStopRow(tSplit.getEndRow());
    trr.setScan(sc);
    trr.setHTable(this.getHTable());
    try {
        trr.initialize(split, context);
    } catch (InterruptedException ex) {
        Logger.getLogger(MyTableInputFormatBase.class.getName()).log(Level.SEVERE, null, ex);
    }
    return trr;
  }

}
