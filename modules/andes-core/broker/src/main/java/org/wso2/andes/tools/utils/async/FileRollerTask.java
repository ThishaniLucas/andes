package org.wso2.andes.tools.utils.async;

import java.io.File;
import java.nio.file.Paths;
import java.sql.Timestamp;

import static org.wso2.andes.configuration.AndesConfigurationManager.CARBON_HOME;

public class FileRollerTask implements Runnable {

    @Override
    public void run() {
        File sourceFile = AsynchronousMessageTracer.getFile();
        File destFile = new File(
                Paths.get(System.getProperty(CARBON_HOME), "repository", "logs", "msg-").toString() + new Timestamp(
                        System.currentTimeMillis()) + ".csv");

        sourceFile.renameTo(destFile);
    }
}
