package nrodclient.stomp.handlers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import nrodclient.NRODClient;
import nrodclient.stomp.NRODListener;
import nrodclient.stomp.StompConnectionHandler;
import org.java_websocket.WebSocket;
import org.json.JSONArray;
import org.json.JSONObject;

public class TDHandler implements NRODListener
{
    private static PrintWriter logStream;
    private static File        logFile;
    private static String      lastLogDate = "";
    private long               lastMessageTime = 0;
    
    private static List<String> areaFilters;

    private static NRODListener instance = null;
    private TDHandler()
    {
        Date logDate = new Date(System.currentTimeMillis());
        logFile = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "TD" + File.separator + NRODClient.sdfDate.format(logDate).replace("/", "-") + ".log");
        logFile.getParentFile().mkdirs();
        lastLogDate = NRODClient.sdfDate.format(logDate);

        try { logStream = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)), true); }
        catch (IOException e) { NRODClient.printThrowable(e, "TD"); }
        
        List<String> filter = new ArrayList<>();
        NRODClient.config.getJSONArray("TD_Area_Filter").forEach(e -> filter.add((String) e));
        filter.sort(null);
        setAreaFilter(filter);
        
        saveTDData(DATA_MAP);

        lastMessageTime = System.currentTimeMillis();
    }
    public static NRODListener getInstance()
    {
        if (instance == null)
            instance = new TDHandler();

        return instance;
    }
    
    public static void setAreaFilter(List<String> newFilter)
    {
        newFilter.sort(null);
        areaFilters = Collections.unmodifiableList(newFilter);
    }

    public static final Map<String, String> DATA_MAP = new ConcurrentHashMap<>();

    @Override
    public void message(Map<String, String> headers, String body)
    {
        StompConnectionHandler.printStompHeaders(headers);

        JSONArray messageList = new JSONArray(body);
        Map<String, String> updateMap = new HashMap<>();

        for (Object mapObj : messageList)
        {
            JSONObject map = (JSONObject) mapObj;
            try
            {
                String msgType = String.valueOf(map.keySet().toArray()[0]);
                JSONObject indvMsg = map.getJSONObject(msgType);
                if (!areaFilters.contains(indvMsg.getString("area_id")))
                    continue;

                String msgAddr = indvMsg.getString("area_id") + indvMsg.optString("address");

                switch (msgType.toUpperCase())
                {
                    case "CA_MSG":
                    {
                        if (!"".equals(DATA_MAP.put(indvMsg.getString("area_id") + indvMsg.getString("from"), "")));
                            updateMap.put(indvMsg.getString("area_id") + indvMsg.getString("from"), "");
                            
                        if (!indvMsg.getString("descr").equals(DATA_MAP.put(indvMsg.getString("area_id") + indvMsg.getString("to"), indvMsg.getString("descr"))));
                            updateMap.put(indvMsg.getString("area_id") + indvMsg.getString("to"), indvMsg.getString("descr"));

                        printTD(String.format("Step %s from %s to %s",
                                indvMsg.getString("descr"),
                                indvMsg.getString("area_id") + indvMsg.getString("from"),
                                indvMsg.getString("area_id") + indvMsg.getString("to")),
                            false,
                            Long.parseLong(indvMsg.getString("time")));
                        break;
                    }
                    
                    case "CB_MSG":
                    {
                        if (!"".equals(DATA_MAP.put(indvMsg.getString("area_id") + indvMsg.getString("from"), "")))
                            updateMap.put(indvMsg.getString("area_id") + indvMsg.getString("from"), "");

                        printTD(String.format("Cancel %s from %s",
                                indvMsg.getString("descr"),
                                indvMsg.getString("area_id") + indvMsg.getString("from")),
                            false,
                            Long.parseLong(indvMsg.getString("time")));
                        break;
                    }
                    
                    case "CC_MSG":
                    {
                        if (!indvMsg.getString("descr").equals(DATA_MAP.put(indvMsg.getString("area_id") + indvMsg.getString("to"), indvMsg.getString("descr"))));
                            updateMap.put(indvMsg.getString("area_id") + indvMsg.getString("to"), indvMsg.getString("descr"));

                        printTD(String.format("Interpose %s to %s",
                                indvMsg.getString("descr"),
                                indvMsg.getString("area_id") + indvMsg.getString("to")),
                            false,
                            Long.parseLong(indvMsg.getString("time")));
                        break;
                    }
                    
                    case "CT_MSG":
                    {
                        updateMap.put("XXHB" + indvMsg.getString("area_id"), indvMsg.getString("report_time"));
                        DATA_MAP.put("XXHB" + indvMsg.getString("area_id"), indvMsg.getString("report_time"));

                        printTD(String.format("Heartbeat from %s at time %s",
                                indvMsg.getString("area_id"),
                                indvMsg.getString("report_time")),
                            false,
                            Long.parseLong(indvMsg.getString("time")));
                        break;
                    }

                    case "SF_MSG":
                    {
                        char[] data = zfill(Integer.parseInt(indvMsg.getString("data"), 16), 8).toCharArray();

                        for (int i = 0; i < data.length; i++)
                        {
                            String address = msgAddr + ":" + Integer.toString(8 - i);
                            String dataBit = String.valueOf(data[i]);
                            
                            if (!DATA_MAP.containsKey(address) || DATA_MAP.get(address) == null || !dataBit.equals(DATA_MAP.get(address)))
                            {
                                printTD(String.format("Change %s from %s to %s",
                                        address,
                                        DATA_MAP.getOrDefault(address, "0"),
                                        dataBit),
                                    false,
                                    Long.parseLong(indvMsg.getString("time")));

                                updateMap.put(address, dataBit);
                            }
                            DATA_MAP.put(address, dataBit);
                        }
                        break;
                    }

                    case "SG_MSG":
                    case "SH_MSG":
                    {
                        String binary = zfill(Long.parseLong(indvMsg.getString("data"), 16), 32);
                        int start = Integer.parseInt(indvMsg.getString("address"), 16);
                        for (int i = 0; i < 4; i++)
                            for (int j = 0; j < 8; j++)
                                updateMap.put(
                                    String.format("%s%s:%s",
                                            indvMsg.getString("area_id"),
                                            zfill(Integer.toHexString(start+i), 2),
                                            8 - j
                                    ).toUpperCase(),
                                    String.valueOf(binary.charAt(8*i+j))
                                );
                        DATA_MAP.putAll(updateMap);
                        break;
                    }
                }
                
            }
            catch (Exception e) { NRODClient.printThrowable(e, "TD"); }
        }
        
        if (NRODClient.guiData != null && NRODClient.guiData.isVisible())
            NRODClient.guiData.updateData();

        if (NRODClient.webSocket != null)
        {
            JSONObject container = new JSONObject();
            JSONObject message = new JSONObject();
            message.put("type", "SEND_UPDATE");
            message.put("timestamp", System.currentTimeMillis());
            message.put("message", updateMap);
            container.put("Message", message);

            String messageStr = container.toString();
            for (WebSocket ws : NRODClient.webSocket.connections())
                if (ws != null && ws.isOpen())
                    ws.send(messageStr);
        }
        saveTDData(updateMap);

        lastMessageTime = System.currentTimeMillis();
        StompConnectionHandler.lastMessageTimeGeneral = lastMessageTime;
        StompConnectionHandler.ack(headers.get("ack"));
    }

    public static String zfill(long l, int len)
    {
        return zfill(String.valueOf(l), len);
    }
    public static String zfill(String s, int len)
    {
        return String.format("%"+len+"s", s).replace(" ", "0");
    }
    
    public static void saveTDData(Map<String, String> mapToSave)
    {
        File TDDataDir = new File(NRODClient.EASM_STORAGE_DIR, "TDData");
        if (!mapToSave.isEmpty())
        {
            mapToSave.keySet().stream().forEach(key ->
            {
                File DataFile = new File(TDDataDir, key.substring(0, 2) + File.separator + key.substring(2).replace(":", "-") + ".td");
                
                if (key.length() != 6)
                    return;
                
                if (!DataFile.exists())
                {
                    try
                    {
                        DataFile.getParentFile().mkdirs();
                        DataFile.createNewFile();
                    }
                    catch (IOException ex) { NRODClient.printThrowable(ex, "TD"); }
                }
                                
                try (BufferedWriter bw = new BufferedWriter(new FileWriter(DataFile, false)))
                {
                    bw.write(mapToSave.getOrDefault(key, ""));
                    bw.newLine();
                }
                catch (IOException ex) { NRODClient.printThrowable(ex, "TD"); }
            });
        }
    }

    public long getTimeout() { return System.currentTimeMillis() - lastMessageTime; }
    public long getTimeoutThreshold() { return 30000; }

    private void printTD(String message, boolean toErr, long timestamp)
    {
        if (NRODClient.verbose)
        {
            if (toErr)
                NRODClient.printErr("[TD] ".concat(message));
            else
                NRODClient.printOut("[TD] ".concat(message));
        }

        if (!lastLogDate.equals(NRODClient.sdfDate.format(new Date())))
        {
            logStream.close();

            Date logDate = new Date();
            lastLogDate = NRODClient.sdfDate.format(logDate);

            logFile = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "TD" + File.separator + NRODClient.sdfDate.format(logDate).replace("/", "-") + ".log");
            logFile.getParentFile().mkdirs();

            try
            {
                logFile.createNewFile();
                logStream = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)), true);
            }
            catch (IOException e) { NRODClient.printThrowable(e, "TD"); }
            
            File fileReplaySave = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "ReplaySaves" + File.separator + NRODClient.sdfDate.format(logDate).replace("/", "-") + ".json");
            fileReplaySave.getParentFile().mkdirs();
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileReplaySave)))
            {
                bw.write(new JSONObject().put("TDData", DATA_MAP).toString());
            }
            catch (IOException e) { NRODClient.printThrowable(e, "TD"); }
        }

        logStream.println("[".concat(NRODClient.sdfDateTime.format(new Date(timestamp))).concat("] ").concat(message));
    }
}