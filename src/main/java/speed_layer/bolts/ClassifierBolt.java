package speed_layer.bolts;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import classifier.SentimentClassifier;

public class ClassifierBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String modelPath;
    private SentimentClassifier classifier;

    public ClassifierBolt(String modelPath) {
        this.modelPath = modelPath;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        File model = new File(modelPath);
        try {
            classifier = new SentimentClassifier(model);
        } catch(IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("keyword", "sentiment"));
    }

    @Override
    public void execute(Tuple tuple) {
        String text = (String) tuple.getValueByField("text");
        ArrayList<String> keywords = (ArrayList<String>) tuple.getValueByField("keywords");
        String sentiment = classifier.classify(text);
        for (String keyword: keywords) {
            collector.emit(new Values(keyword, sentiment));
        }
    }
    
}