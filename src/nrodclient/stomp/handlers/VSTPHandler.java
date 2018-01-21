package nrodclient.stomp.handlers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Map;
import nrodclient.NRODClient;
import nrodclient.stomp.NRODListener;
import nrodclient.stomp.StompConnectionHandler;
import org.json.JSONObject;

public class VSTPHandler implements NRODListener
{
    private static PrintWriter logStream;
    private static File        logFile;
    private static String      lastLogDate = "";
    private        long        lastMessageTime = 0;

    private static NRODListener instance = null;
    private VSTPHandler()
    {
        Date logDate = new Date(System.currentTimeMillis());
        logFile = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "VSTP" + File.separator + NRODClient.sdfDate.format(logDate).replace("/", "-") + ".log");
        logFile.getParentFile().mkdirs();
        lastLogDate = NRODClient.sdfDate.format(logDate);

        try { logStream = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)), true); }
        catch (IOException e) { NRODClient.printThrowable(e, "VSTP"); }
        
        lastMessageTime = System.currentTimeMillis();
    }
    public static NRODListener getInstance()
    {
        if (instance == null)
            instance = new VSTPHandler();

        return instance;
    }

    @Override
    public void message(Map<String, String> headers, String message)
    {
        StompConnectionHandler.printStompHeaders(headers);

        JSONObject msg = new JSONObject(message).getJSONObject("VSTPCIFMsgV1");
        printVSTP(message, false, msg.optLong("timestamp", 0L));

        lastMessageTime = System.currentTimeMillis();
        StompConnectionHandler.lastMessageTimeGeneral = lastMessageTime;
        StompConnectionHandler.ack(headers.get("ack"));
    }

    public long getTimeout() { return System.currentTimeMillis() - lastMessageTime; }
    public long getTimeoutThreshold() { return 3600000; }

    private static void printVSTP(String message, boolean toErr, long timestamp)
    {
        if (NRODClient.verbose)
        {
            if (toErr)
                NRODClient.printErr("[VSTP] " + message);
            else
                NRODClient.printOut("[VSTP] " + message);
        }
        
        if (!lastLogDate.equals(NRODClient.sdfDate.format(new Date())))
        {
            logStream.close();

            Date logDate = new Date();
            lastLogDate = NRODClient.sdfDate.format(logDate);

            logFile = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "VSTP" + File.separator + NRODClient.sdfDate.format(logDate).replace("/", "-") + ".log");
            logFile.getParentFile().mkdirs();

            try
            {
                logFile.createNewFile();
                logStream = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)), true);
            }
            catch (IOException e) { NRODClient.printThrowable(e, "VSTP"); }
        }
        logStream.println("[".concat(NRODClient.sdfDateTime.format(new Date(timestamp))).concat("] ").concat(message));
    }
}