package speed_layer.spouts;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import jodd.util.StringUtil;
import com.opencsv.CSVReader;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class FullCorpusSpout extends BaseRichSpout {
	
	public static final String FILE = "datasets/full-corpus.csv";

    private SpoutOutputCollector collector;
    private List<List<String>> records;

    public FullCorpusSpout() throws IOException {
        this.records = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(FILE))) {
            String[] values;
            while ((values = reader.readNext()) != null) {
                this.records.add(Arrays.asList(values));
            }
        }
        this.records.remove(0);
        Collections.shuffle(records);
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("tweet_ID", "text", "keywords"));
    }

    @Override
    public void nextTuple(){
        for (List<String> record: records) {
            if (!record.get(1).equals("irrelevant") || !record.get(1).equals("neutral")) {
                ArrayList<String> keywords = new ArrayList<>();
                keywords.add("#" + StringUtil.capitalize(record.get(0)));
                String tweet_ID = record.get(2);
                String text = record.get(4);
                collector.emit(new Values(tweet_ID, text, keywords));
                Utils.sleep(1000);
            }
        }
    }

}
