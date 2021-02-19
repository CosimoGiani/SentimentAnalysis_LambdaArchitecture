package batch_layer;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class Driver extends Configured implements Tool {
	
	public static final String CLASSIFIER_MODEL = "ClassifierModel.model";

    @Override
    public int run(String[] args) throws Exception {

        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("content"));

        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        Table table = connection.getTable(TableName.valueOf("synchronization"));

        table.put(new Put(Bytes.toBytes("MapReduce_start_timestamp")).addColumn(Bytes.toBytes("placeholder"), Bytes.toBytes(""), Bytes.toBytes("")));

        long start = System.currentTimeMillis();
        conf.setLong("start", start);

        Job job = Job.getInstance(conf, "SentimentAnalysis");
        job.addCacheFile(new URI(CLASSIFIER_MODEL));
        job.setJarByClass(Driver.class);

        TableMapReduceUtil.initTableMapperJob("master_database", scan, Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("batch_view", Reducer.class, job);

        System.out.println("Start: " + start);

        int returnValue = (job.waitForCompletion(true)) ? 0 : 1;

        table.put(new Put(Bytes.toBytes("MapReduce_end_timestamp")).addColumn(Bytes.toBytes("placeholder"), Bytes.toBytes(""), Bytes.toBytes("")));

        return returnValue;
        
    }

    public static void main(String[] args) throws Exception {
        while (true) {
            ToolRunner.run(new Driver(), args);
            Thread.sleep(30 * 1000);
        }
    }

}
