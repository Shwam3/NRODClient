package nrodclient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import nrodclient.NRODClient;
import nrodclient.stomp.NRODListener;
import nrodclient.stomp.StompConnectionHandler;

public class RateMonitor implements NRODListener
{
    private final Map<String, AtomicInteger> rateMap = new HashMap<>();
    private static PrintWriter logStream;
    private static File        logFile;
    private static String      lastLogDate = "";
    private final  String[]    topics = {"/topic/TRAIN_MVT_ALL_TOC", "/topic/RTPPM_ALL", "/topic/VSTP_ALL", "/topic/TSR_ALL_ROUTE", "/topic/TD_ALL_SIG_AREA"};

    private static NRODListener instance = null;
    private RateMonitor()
    {
        String logDate = NRODClient.sdfDate.format(new Date());
        logFile = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "RateMonitor" + File.separator + logDate.replace("/", "-") + ".csv");
        boolean fileExisted = logFile.exists();
        logFile.getParentFile().mkdirs();
        lastLogDate = logDate;

        try { logStream = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)), true); }
        catch (IOException e) { NRODClient.printThrowable(e, "RateMonitor"); }

        if (!fileExisted)
        {
            logStream.print("time,");
            for (int i = 0; i < topics.length; i++)
                logStream.print(topics[i] + (i >= topics.length - 1 ? "" : ","));
            logStream.println();
        }

        for (String topic : topics)
            rateMap.put(topic, new AtomicInteger(0));

        long currTim = System.currentTimeMillis();
        Calendar wait = Calendar.getInstance();
        wait.setTimeInMillis(currTim);
        wait.set(Calendar.MILLISECOND, 0);
        wait.set(Calendar.SECOND, 0);
        wait.add(Calendar.MINUTE, 1);

        ScheduledFuture<?> sf = Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() ->
        {
            String newDate = NRODClient.sdfDate.format(new Date());
            if (!lastLogDate.equals(newDate))
            {
                logStream.close();

                lastLogDate = newDate;
                logFile = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "RateMonitor" + File.separator + newDate.replace("/", "-") + ".csv");
                logFile.getParentFile().mkdirs();

                try { logStream = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)), true); }
                catch (IOException e) { NRODClient.printThrowable(e, "RateMonitor"); }

                logStream.print("time,");
                for (int i = 0; i < topics.length; i++)
                    logStream.print(topics[i] + (i >= topics.length - 1 ? "" : ","));
                logStream.println();
            }

            logStream.print(NRODClient.sdfTime.format(new Date()) + ",");
            StompConnectionHandler.printStomp("Rate Monitor", false);
            for (int i = 0; i < topics.length; i++)
            {
                String topic = topics[i];
                long count = rateMap.get(topic).getAndSet(0);
                StompConnectionHandler.printStomp("  \"" + topic + "\": " + count, false);
                logStream.print(count + (i >= topics.length-1 ? "" : ","));
            }
            logStream.println();
        }, wait.getTimeInMillis() - currTim, 1000 * 60, TimeUnit.MILLISECONDS);
    }

    public static NRODListener getInstance()
    {
        if (instance == null)
            instance = new RateMonitor();

        return instance;

    }

    @Override
    public void message(Map<String, String> headers, String body)
    {
        String topic = headers.get("destination").replace("\\c", ":");

        rateMap.get(topic).incrementAndGet();
    }
}