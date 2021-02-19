package speed_layer.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStreamSpout extends BaseRichSpout {
	
	private TwitterStream stream;
    private LinkedBlockingQueue<Status> tweets = null;
    private SpoutOutputCollector collector;

    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
    private String[] keywords;

    public TwitterStreamSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret, String[] keywords) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
        this.keywords = keywords;
    }

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        tweets = new LinkedBlockingQueue<Status>(1000);

        StatusListener listener = new StatusListener() {
            public void onStatus(Status status){
                tweets.offer(status);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

            public void onTrackLimitationNotice(int i) {}

            public void onScrubGeo(long l, long l1) {}

            public void onStallWarning(StallWarning stallWarning) {}

            public void onException(Exception e) {}
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();

        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setOAuthAccessToken(accessToken);
        cb.setOAuthAccessTokenSecret(accessTokenSecret);

        this.stream = new TwitterStreamFactory(cb.build()).getInstance();
        stream.addListener(listener);

        if (keywords.length == 0) {
            stream.sample();
        }
        else {
            FilterQuery query = new FilterQuery().track(keywords);
            stream.filter(query);
        }

    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    @Override
    public void nextTuple() {
        Status tweet = tweets.poll();
        if (tweet == null) {
            Utils.sleep(50);
        }
        else {
            if (tweet.getLang().equals("en")) {
                collector.emit(new Values(tweet));
            }
        }
    }

    @Override
    public void close() {
        stream.shutdown();
    }

}
