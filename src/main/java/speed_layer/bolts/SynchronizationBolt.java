package speed_layer.bolts;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class SynchronizationBolt extends BaseRichBolt {
	
    private String tableName;
    private Table table;

    public SynchronizationBolt(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext context, OutputCollector collector) {
        try {
            Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
            this.table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void execute(Tuple tuple) {
        long timestamp = (long)tuple.getValueByField("timestamp");
        try {
            Scan scan = new Scan().setTimeRange(0, timestamp);
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
                Delete delete = new Delete(result.getRow());
                table.delete(delete);
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

}