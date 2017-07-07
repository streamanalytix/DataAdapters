/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.channel;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamanalytix.framework.api.spark.channel.BaseChannel;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;

/** The Class RSSFeedChannel. */
public class RSSFeedChannel extends BaseChannel implements Serializable {

    private static final String FEED_URL = "Feed_URL";
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -8657585005598090114L;
    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(RSSFeedChannel.class);
    /** Feed url. */
    private String feedUrl;

    /** rssFeed receiver. */
    private RssReceiver rssReceiver;

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#init(java.util.Map)
     */
    @Override
    public void init(Map<String, Object> configMap) {
        if (configMap.containsKey(FEED_URL)) {
            feedUrl = (String) configMap.get(FEED_URL);
        }

        this.rssReceiver = new RssReceiver();
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.Channel#getDStream(org. apache.spark.streaming.api.java.JavaStreamingContext)
     */
    @Override
    public JavaDStream<Object> getDStream(JavaStreamingContext context) {

        JavaReceiverInputDStream<String> result = context.receiverStream(this.rssReceiver);
        JavaDStream<Object> finalResult = result.map(new Function<String, Object>() {
            private static final long serialVersionUID = 7522170517856295596L;

            @Override
            public Object call(String arg0) throws Exception {
                return arg0.getBytes();
            }
        });

        return finalResult;
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.BaseChannel#getRDD(org.apache.spark.api.java.JavaSparkContext)
     */
    @Override
    public JavaRDD<Object> getRDD(JavaSparkContext context) {
        return context.textFile(this.feedUrl).map(new Function<String, Object>() {
            private static final long serialVersionUID = 335729824771195544L;

            @Override
            public Object call(String arg0) throws Exception {
                return arg0.getBytes();
            }
        });
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#cleanup()
     */
    @Override
    public void cleanup() {
    }

    /** The Class RssReceiver. */
    class RssReceiver extends Receiver<String> {

        private static final int _10000 = 10000;
        /** The Constant serialVersionUID. */
        private static final long serialVersionUID = 1L;

        /** Instantiates a new rss receiver. */
        public RssReceiver() {
            super(StorageLevel.MEMORY_AND_DISK_2());
        }

        /*
         * (non-Javadoc)
         * @see org.apache.spark.streaming.receiver.Receiver#onStart()
         */
        @Override
        public void onStart() {
            // Start the thread that receives data over a connection
            new Thread() {
                @Override
                public void run() {
                    try {
                        receive();
                    } catch (InterruptedException ex) {
                        LOGGER.error("Exception occurred: " + ex);
                    }
                }
            }.start();

        }

        /*
         * (non-Javadoc)
         * @see org.apache.spark.streaming.receiver.Receiver#onStop()
         */
        @Override
        public void onStop() {
        }

        /** Receive.
         *
         * @throws InterruptedException
         *             the interrupted exception */
        private void receive() throws InterruptedException {
            try {
                String title = null;
                Date pubDate = null;
                String tempTitle = null;
                Date tempPubDate = null;
                ObjectMapper mapper = new ObjectMapper();
                while (!isStopped()) {
                    URL url = new URL(feedUrl);

                    SyndFeedInput input = new SyndFeedInput();
                    SyndFeed sf = input.build(new XmlReader(url));

                    List<SyndEntry> entries = sf.getEntries();
                    if (entries != null && entries.size() > 0) {
                        long index = 0;
                        for (SyndEntry entry : entries) {
                            if (entry.getTitle().equalsIgnoreCase(title) && entry.getPublishedDate().equals(pubDate))
                                break;
                            String item = mapper.writeValueAsString(entry);
                            store(item);
                            if (index == 0) {
                                tempTitle = entry.getTitle();
                                tempPubDate = entry.getPublishedDate();
                            }
                            index++;
                        }
                        title = tempTitle;
                        pubDate = tempPubDate;
                    }
                    Thread.sleep(_10000);
                }
            } catch (IOException | FeedException ex) {
                LOGGER.error("Exception occurred while receiving message: " + ex);
                restart(ex.getMessage(), ex);
            }

        }
    }
}
