package batch_layer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import classifier.SentimentClassifier;

public class Mapper extends TableMapper<Text, Text> {
	
    private SentimentClassifier classifier;
    private long startTimestamp;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        startTimestamp = Long.parseLong(configuration.get("start"));
        File model = new File(context.getCacheFiles()[0].toString());
        try {
            classifier = new SentimentClassifier(model);
        } catch(ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        long check = result.rawCells()[0].getTimestamp();
        if (check <= startTimestamp) {
            byte[] byteText = result.getValue(Bytes.toBytes("content"), Bytes.toBytes("text"));
            String text = new String(byteText);
            byte[] byteKeywords = result.getValue(Bytes.toBytes("content"), Bytes.toBytes("keywords"));
            ArrayWritable writable = new ArrayWritable(Text.class);
            writable.readFields(new DataInputStream(new ByteArrayInputStream(byteKeywords)));
            ArrayList<String> keywords = fromWritable(writable);
            String sentiment = classifier.classify(text);
            for (String keyword: keywords) {
                context.write(new Text(keyword), new Text(sentiment));
            }
        }
    }
    
    private static ArrayList<String> fromWritable(ArrayWritable writable) {
        Writable[] writables = ((ArrayWritable) writable).get();
        ArrayList<String> list = new ArrayList<String>(writables.length);
        for (Writable w : writables) {
            list.add(((Text)w).toString());
        }
        return list;
    }

}
