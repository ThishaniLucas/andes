package org.wso2.andes.tools.utils.async;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.wso2.andes.configuration.AndesConfigurationManager.CARBON_HOME;

public class AsynchronousMessageTracer {
    private static Log log = LogFactory.getLog(AsynchronousMessageTracer.class);
    private static BlockingQueue<LogEventFormat> eventQueue = new LinkedBlockingQueue<>();
    private static ScheduledExecutorService executorFileWriter = Executors.newScheduledThreadPool(1);
    private static ScheduledExecutorService executorFileRoller = Executors.newScheduledThreadPool(1);
    private static final File file = new File(Paths.get(System.getProperty(CARBON_HOME), "repository", "logs", "msg.csv").toString());

    public static void trace(long timeStamp, String jmsProperties, String user, String msgId, String destination, TraceMessageStatus state) {
        LogEventFormat event = new LogEventFormat(timeStamp, jmsProperties, user, msgId, destination, state);
        try {
            eventQueue.put(event);
        } catch (InterruptedException e) {
            if(log.isDebugEnabled()){
                log.error("Couldn't add log event to queue!");
            }
        }
    }

    public static Queue getQueue() {
        return eventQueue;
    }

    public static File getFile() {
        return file;
    }

    static {
        executorFileWriter.scheduleAtFixedRate(new FileWriterTask(), 0, 10, TimeUnit.SECONDS);
        executorFileRoller.scheduleAtFixedRate(new FileRollerTask(), 0, 20, TimeUnit.SECONDS);
    }

}
