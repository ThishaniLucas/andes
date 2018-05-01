package org.wso2.andes.tools.utils.async;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Queue;

public class FileWriterTask implements Runnable {
    private static Log log = LogFactory.getLog(AsynchronousMessageTracer.class);

    @Override
    public void run() {
        Queue<LogEventFormat> eventQueue = AsynchronousMessageTracer.getQueue();
        File file = AsynchronousMessageTracer.getFile();
        try {
            FileWriter writer = new FileWriter(file, true);
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);

            while (!eventQueue.isEmpty()) {
                LogEventFormat logEventFormat = eventQueue.poll();
                csvPrinter.printRecord(new Timestamp(logEventFormat.getTimeStamp()), logEventFormat.getMsgId(),
                        logEventFormat.getState());
            }
            csvPrinter.flush();
        } catch (IOException e) {
            if(log.isDebugEnabled()){
                log.error("Couldn't write to file!");
            }
        }
    }
}
