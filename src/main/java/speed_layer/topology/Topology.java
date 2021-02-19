package speed_layer.topology;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.shade.com.google.common.io.Files;
import org.apache.storm.topology.TopologyBuilder;

import speed_layer.spouts.*;
import speed_layer.bolts.*;

public class Topology {
	
	public static void main(String[] args) throws Exception {

        // Read the credential for the Twitter API 
        List<String> lines = Files.readLines(new File("TwitterCredentials"), Charset.defaultCharset());
        String consumerKey = Arrays.asList(lines.get(0).split(":")).get(1).trim();
        String consumerSecret = Arrays.asList(lines.get(1).split(":")).get(1).trim();
        String accessToken = Arrays.asList(lines.get(2).split(":")).get(1).trim();
        String accessTokenSecret = Arrays.asList(lines.get(3).split(":")).get(1).trim();

        // Read query keywords
        String[] arguments = args.clone();
        String[] keywords = Arrays.copyOfRange(arguments, 0, arguments.length);

        Config config = new Config();
        config.setDebug(true);

        // Creating the tables of the serving layer
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        
        if(!admin.tableExists(TableName.valueOf("real_time_database"))) {
            TableDescriptor realTimeView = TableDescriptorBuilder.newBuilder(TableName.valueOf("real_time_database"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("content".getBytes()).build())
                    .build();
            admin.createTable(realTimeView);
        }

        if(!admin.tableExists(TableName.valueOf("synchronization"))){
            TableDescriptor synchronization = TableDescriptorBuilder.newBuilder(TableName.valueOf("synchronization"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("placeholder".getBytes()).build())
                    .build();
            admin.createTable(synchronization);
            Table table = connection.getTable(TableName.valueOf("synchronization"));
            table.put(new Put(Bytes.toBytes("MapReduce_start_timestamp"), 0)
                    .addColumn(Bytes.toBytes("placeholder"), Bytes.toBytes(""), Bytes.toBytes("")));
            table.put(new Put(Bytes.toBytes("MapReduce_end_timestamp"), 0)
                    .addColumn(Bytes.toBytes("placeholder"), Bytes.toBytes(""), Bytes.toBytes("")));
        }
        
        if(!admin.tableExists(TableName.valueOf("master_database"))){
            TableDescriptor masterDatabase = TableDescriptorBuilder.newBuilder(TableName.valueOf("master_database"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("content".getBytes()).build())
                    .build();
            admin.createTable(masterDatabase);
        }


        if(!admin.tableExists(TableName.valueOf("batch_view"))){
            TableDescriptor batchView = TableDescriptorBuilder.newBuilder(TableName.valueOf("batch_view"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("sentiment_count".getBytes()).build())
                    .build();
            admin.createTable(batchView);
        }

        // Topology 
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitter-stream-spout", new TwitterStreamSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keywords), 1);
        
        builder.setSpout("full-corpus-spout", new FullCorpusSpout(), 1);

        builder.setBolt("parser-bolt", new ParserBolt(keywords), 3).shuffleGrouping("twitter-stream-spout");
        
        builder.setBolt("classifier-bolt", new ClassifierBolt("ClassifierModel.model"), 3)
        		.shuffleGrouping("parser-bolt").shuffleGrouping("full-corpus-spout");

        builder.setBolt("master-database-bolt", new MasterDatabaseBolt("master_database"), 3)
                .shuffleGrouping("parser-bolt").shuffleGrouping("full-corpus-spout");

        builder.setBolt("realtime-database-bolt", new RealTimeDatabaseBolt("real_time_database"), 3).shuffleGrouping("classifier-bolt");

        builder.setSpout("synchronization-spout", new SynchronizationSpout("synchronization"), 1);

        builder.setBolt("synchronization-bolt", new SynchronizationBolt("real_time_database"), 3).shuffleGrouping("synchronization-spout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("StormTopology", config, builder.createTopology());
        Thread.sleep(1200000);
        cluster.shutdown();
        
    }
	
}
