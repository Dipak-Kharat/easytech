import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class Asach {
    public static void main(String[] args) throws InterruptedException {

        LinkedBlockingQueue<Status> lq=new LinkedBlockingQueue();
        String cunsumerKey=args[0];
        String cunsumerSecret=args[1];
        String accessToken=args[2];
        String accessTokenSecret=args[3];
        String topicName=args[4];
        String[] arguments=args.clone();
        String[] Keywords= Arrays.copyOfRange(arguments,5,arguments.length);
        ConfigurationBuilder cb=new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(cunsumerKey)
                .setOAuthConsumerSecret(cunsumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret);
        TwitterStream TS=new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener SL=new StatusListener() {
            @Override
            public void onStatus(Status status) {
                lq.offer(status);

//                for (HashtagEntity ht:status.getHashtagEntities()){
//                    System.out.println(ht.getText());
//                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            @Override
            public void onException(Exception e) {

            }
        };
        TS.addListener(SL);
        FilterQuery fQ=new FilterQuery().track(Keywords);
        TS.filter(fQ);
        Thread.sleep(5000);


        Properties props=new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks","all");
        props.put( "key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");


        Producer<String, String> ps=new KafkaProducer<String,String>(props);

        int i=0;
        int j=0;

        while(i < 10){

            Status ret = lq.poll();

            if(ret == null){
                Thread.sleep(100);
                i++;
            }
            else {
                for(HashtagEntity hashtagEntity:ret.getHashtagEntities()){
                    System.out.println("Hashtag: " + hashtagEntity.getText());
                    ps.send(new ProducerRecord<String,String>(topicName,Integer.toString(j++),hashtagEntity.getText()));

                }
            }
        }
        ps.close();
        Thread.sleep(5000);
        TS.shutdown();

    }
}

