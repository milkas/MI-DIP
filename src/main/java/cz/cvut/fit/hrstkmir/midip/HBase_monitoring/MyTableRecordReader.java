package cz.cvut.fit.hrstkmir.midip.HBase_monitoring;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.mapreduce.TableRecordReader;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

/**
 *
 * @author mira
 */
public class MyTableRecordReader extends TableRecordReader {

    static final Log LOG = LogFactory.getLog(TableRecordReader.class);

  // HBASE_COUNTER_GROUP_NAME is the name of mapreduce counter group for HBase
  private static final String HBASE_COUNTER_GROUP_NAME =
    "HBase Counters";
  private ResultScanner scanner = null;
  private Scan scan = null;
  private Scan currentScan = null;
  private HTable htable = null;
  private byte[] lastSuccessfulRow = null;
  private ImmutableBytesWritable key = null;
  private Result value = null;
  private TaskAttemptContext context = null;
  private Method getCounter = null;


    java.util.Date date = new java.util.Date();
    public File absolute = new File("/home/mira/metadata/file-" + new Timestamp(date.getTime()) + ".txt");
    FileWriter fw;
    BufferedWriter bw;
    public long rc = 0;
    public HashMap<String, List<String>> map = new HashMap<>();
    public List<String> tmpList;

    public MyTableRecordReader() throws IOException {
        fw = new FileWriter(absolute.getAbsoluteFile());
        bw = new BufferedWriter(fw);
    }

    /**
     * Restart from survivable exceptions by creating a new scanner.
     *
     * @param firstRow The first row to start at.
     * @throws IOException When restarting fails.
     */
    @Override
    public void restart(byte[] firstRow) throws IOException {
            currentScan = new Scan(scan);
    currentScan.setStartRow(firstRow);
    currentScan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE,
      Bytes.toBytes(Boolean.TRUE));
    this.scanner = this.htable.getScanner(currentScan);
    }
    
      private Method retrieveGetCounterWithStringsParams(TaskAttemptContext context)
  throws IOException {
    Method m = null;
    try {
      m = context.getClass().getMethod("getCounter",
        new Class [] {String.class, String.class});
    } catch (SecurityException e) {
      throw new IOException("Failed test for getCounter", e);
    } catch (NoSuchMethodException e) {
      // Ignore
    }
    return m;
  }

    /**
     * Sets the HBase table.
     *
     * @param htable The {@link HTable} to scan.
     */
      @Override
    public void setHTable(HTable htable) {
         this.htable = htable;
    }

    /**
     * Sets the scan defining the actual details like columns etc.
     *
     * @param scan The scan to set.
     */
    @Override
    public void setScan(Scan scan) {
        this.scan = scan;
    }

    /**
     * Closes the split.
     *
     * @see org.apache.hadoop.mapreduce.RecordReader#close()
     */
    @Override
    public void close() {
        try {
            for(Map.Entry<String, List<String>> entry : map.entrySet())
            {
             
                    //improvizace pro pripad ze v textu je strednik, ktery by pak rozhodil parsovani
                    String key2 = entry.getKey();
                    if(key2.contains(";"))
                        key2.replace(";", "semicolon");
                    
                    bw.write(entry.getKey()+";");  //lip se to parsuje...
                    tmpList = (List<String>) entry.getValue();
                    bw.write(tmpList.size()+";");
                    
                    Iterator<String> iterator = tmpList.iterator();
                    while (iterator.hasNext())
                    {
                        bw.write(iterator.next()+" ");
                    }
                    bw.newLine();

            }
            
            bw.close();
            fw.close();
            this.scanner.close();
        } catch (IOException ex) {
            Logger.getLogger(MyTableRecordReader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Returns the current key.
     *
     * @return The current key.
     * @throws IOException
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
     */
    @Override
    public ImmutableBytesWritable getCurrentKey() throws IOException,
            InterruptedException {
            return key;
    }

    /**
     * Returns the current value.
     *
     * @return The current value.
     * @throws IOException When the value is faulty.
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
     */
    @Override
    public Result getCurrentValue() throws IOException, InterruptedException {
       return value;
    }

    /**
     * Initializes the reader.
     *
     * @param inputsplit The split to work with.
     * @param context The current task context.
     * @throws IOException When setting up the reader fails.
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#initialize(
     * org.apache.hadoop.mapreduce.InputSplit,
     * org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void initialize(InputSplit inputsplit,
            TaskAttemptContext context) throws IOException,
            InterruptedException {

    if (context != null) {
      this.context = context;
      getCounter = retrieveGetCounterWithStringsParams(context);
    }
    restart(scan.getStartRow());
    }

    /**
     * Positions the record reader to the next record.
     *
     * @return <code>true</code> if there was another record.
     * @throws IOException When reading the record failed.
     * @throws InterruptedException When the job was aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
         if (key == null) key = new ImmutableBytesWritable();
    if (value == null) value = new Result();
    try {
      value = this.scanner.next();
    } catch (DoNotRetryIOException e) {
      throw e;
    } catch (IOException e) {
      LOG.info("recovered from " + StringUtils.stringifyException(e));
      if (lastSuccessfulRow == null) {
        LOG.warn("We are restarting the first next() invocation," +
            " if your mapper's restarted a few other times like this" +
            " then you should consider killing this job and investigate" +
            " why it's taking so long.");
      }
      if (lastSuccessfulRow == null) {
        restart(scan.getStartRow());
      } else {
        restart(lastSuccessfulRow);
        scanner.next();    // skip presumed already mapped row
      }
      value = scanner.next();
    }
    if (value != null && value.size() > 0) {
      key.set(value.getValue(Bytes.toBytes("osoba"), Bytes.toBytes("kraj")));
      rc++;
      String rowID = new String(value.getRow());
                                    
                                String tmp = new String (key.copyBytes());
     				if(map.containsKey(tmp))
				{
					tmpList = map.get(tmp);
					tmpList.add(rowID);
					map.put(tmp, tmpList);
				}
				else
				{
					tmpList = new ArrayList<String>();
					tmpList.add(rowID);
					map.put(tmp, tmpList);		
				} 
      
      lastSuccessfulRow = key.get();
      return true;
    }

    updateCounters();
    return false;
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return A number between 0.0 and 1.0, the fraction of the data read.
     * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
     */
    @Override
  public float getProgress() {
    // Depends on the total number of tuples
    return 0;
  }
    
    private void updateCounters() throws IOException {
    // we can get access to counters only if hbase uses new mapreduce APIs
    if (this.getCounter == null) {
      return;
    }

    byte[] serializedMetrics = currentScan.getAttribute(
        Scan.SCAN_ATTRIBUTES_METRICS_DATA);
    if (serializedMetrics == null || serializedMetrics.length == 0 ) {
      return;
    }

    DataInputBuffer in = new DataInputBuffer();
    in.reset(serializedMetrics, 0, serializedMetrics.length);
  }
}
