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
import org.json.JSONArray;
import org.json.JSONObject;

public class TDHandler implements NRODListener
{
    private static PrintWriter logOldStream;
    private static File        logOldFile;
    private static PrintWriter logNewStream;
    private static File        logNewFile;
    private static String      lastLogDate = "";
    private        long        lastMessageTime = 0;
    
    private static List<String> areaFilters;

    private static NRODListener instance = null;
    private TDHandler()
    {
        lastLogDate = NRODClient.sdfDate.format(new Date());
        
        logOldFile = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "TDOld" + File.separator + lastLogDate.replace("/", "-") + ".log");
        logOldFile.getParentFile().mkdirs();
        try { logOldStream = new PrintWriter(new BufferedWriter(new FileWriter(logOldFile, true)), true); }
        catch (IOException e) { NRODClient.printThrowable(e, "TD"); }
        
        logNewFile = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "TD" + File.separator + lastLogDate.replace("/", "-") + ".log");
        logNewFile.getParentFile().mkdirs();
        try { logNewStream = new PrintWriter(new BufferedWriter(new FileWriter(logNewFile, true)), true); }
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
                // normally only one message per key
                for (String msgType : map.keySet())
                {
                    JSONObject indvMsg = map.getJSONObject(msgType);
                    boolean isInFilter = !areaFilters.contains(indvMsg.getString("area_id"));

                    String msgAddr = indvMsg.getString("area_id") + indvMsg.optString("address");

                    switch (msgType.toUpperCase())
                    {
                        case "CA_MSG":
                        {
                            updateMap.put(indvMsg.getString("area_id") + indvMsg.getString("from"), "");
                            updateMap.put(indvMsg.getString("area_id") + indvMsg.getString("to"), indvMsg.getString("descr"));

                            if (isInFilter)
                                printTD(String.format("Step %s from %s to %s",
                                        indvMsg.getString("descr"),
                                        indvMsg.getString("area_id") + indvMsg.getString("from"),
                                        indvMsg.getString("area_id") + indvMsg.getString("to")),
                                    false,
                                    Long.parseLong(indvMsg.getString("time")));
                            printTD(String.format("CA %s %s %s",
                                    indvMsg.getString("descr"),
                                    indvMsg.getString("area_id") + indvMsg.getString("from"),
                                    indvMsg.getString("area_id") + indvMsg.getString("to")),
                                true,
                                Long.parseLong(indvMsg.getString("time")));
                            break;
                        }

                        case "CB_MSG":
                        {
                            updateMap.put(indvMsg.getString("area_id") + indvMsg.getString("from"), "");

                            if (isInFilter)
                                printTD(String.format("Cancel %s from %s",
                                        indvMsg.getString("descr"),
                                        indvMsg.getString("area_id") + indvMsg.getString("from")),
                                    false,
                                    Long.parseLong(indvMsg.getString("time")));
                            printTD(String.format("CB %s %s",
                                    indvMsg.getString("descr"),
                                    indvMsg.getString("area_id") + indvMsg.getString("from")),
                                true,
                                Long.parseLong(indvMsg.getString("time")));
                            break;
                        }

                        case "CC_MSG":
                        {
                            String to = indvMsg.getString("area_id") + indvMsg.getString("to");
                            updateMap.put(to, indvMsg.getString("descr"));

                            if (isInFilter)
                                printTD(String.format("Interpose %s to %s",
                                        indvMsg.getString("descr"),
                                        to),
                                    false,
                                    Long.parseLong(indvMsg.getString("time")));
                            printTD(String.format("CC %s %s%s",
                                    indvMsg.getString("descr"),
                                    indvMsg.getString("area_id") + indvMsg.getString("to"),
                                    "".equals(DATA_MAP.getOrDefault(to, "")) ?
                                        "" : (" " + DATA_MAP.get(to))),
                                true,
                                Long.parseLong(indvMsg.getString("time")));
                            break;
                        }

                        case "CT_MSG":
                        {
                            updateMap.put("XXHB" + indvMsg.getString("area_id"), indvMsg.getString("report_time"));

                            if (isInFilter)
                                printTD(String.format("Heartbeat from %s at time %s",
                                        indvMsg.getString("area_id"),
                                        indvMsg.getString("report_time")),
                                    false,
                                    Long.parseLong(indvMsg.getString("time")));
                            printTD(String.format("CT %s %s",
                                    indvMsg.getString("area_id"),
                                    indvMsg.getString("report_time")),
                                true,
                                Long.parseLong(indvMsg.getString("time")));
                            break;
                        }

                        case "SF_MSG":
                        {
                            char[] data = zfill(Integer.toBinaryString(Integer.parseInt(indvMsg.getString("data"), 16)), 8).toCharArray();

                            for (int i = 0; i < data.length; i++)
                            {
                                String address = msgAddr + ":" + Integer.toString(8 - i);
                                String dataBit = String.valueOf(data[i]);

                                if (!DATA_MAP.containsKey(address) || DATA_MAP.get(address) == null || !dataBit.equals(DATA_MAP.get(address)))
                                {
                                    if (isInFilter)
                                        printTD(String.format("Change %s from %s to %s",
                                                address,
                                                DATA_MAP.getOrDefault(address, "0"),
                                                dataBit),
                                            false,
                                            Long.parseLong(indvMsg.getString("time")));
                                    printTD(String.format("SF %s %s %s",
                                            address,
                                            DATA_MAP.getOrDefault(address, "0"),
                                            dataBit),
                                        true,
                                        Long.parseLong(indvMsg.getString("time")));

                                    updateMap.put(address, dataBit);
                                }
                            }
                            break;
                        }

                        case "SG_MSG":
                        case "SH_MSG":
                        {
                            String binary = zfill(Long.toBinaryString(Long.parseLong(indvMsg.getString("data"), 16)), 32);
                            int start = Integer.parseInt(indvMsg.getString("address"), 16);
                            for (int i = 0; i < 4; i++)
                                for (int j = 0; j < 8; j++)
                                {
                                    String id = String.format("%s%s:%s",
                                                    indvMsg.getString("area_id"),
                                                    zfill(Integer.toHexString(start+i), 2),
                                                    8 - j
                                                ).toUpperCase();
                                    String dat = String.valueOf(binary.charAt(8 * i + j));
                                    updateMap.put(id, dat);
                                    if (!DATA_MAP.containsKey(id) || DATA_MAP.get(id) == null || dat.equals(DATA_MAP.get(id)))
                                    {
                                        if (isInFilter)
                                            printTD(String.format("Change %s from %s to %s",
                                                    id,
                                                    DATA_MAP.getOrDefault(id, "0"),
                                                    dat
                                                ),
                                                false,
                                                Long.parseLong(indvMsg.getString("time")));
                                        printTD(String.format("%s %s %s %s",
                                                indvMsg.get("msg_type"),
                                                id,
                                                DATA_MAP.getOrDefault(id, "0"),
                                                dat
                                            ),
                                            true,
                                            Long.parseLong(indvMsg.getString("time")));
                                    }
                                }
                            break;
                        }
                    }
                }
            }
            catch (Exception e) { NRODClient.printThrowable(e, "TD"); }
        }
        
        DATA_MAP.putAll(updateMap);
        
        if (NRODClient.guiData != null && NRODClient.guiData.isVisible())
            NRODClient.guiData.updateData();

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
            JSONObject cClObj = new JSONObject();
            JSONObject sClObj = new JSONObject();
            
            mapToSave.keySet().forEach(key ->
            {
                String area = key.substring(0, 2);
                if (key.charAt(4) == ':')
                {
                    if (sClObj.has(area))
                        sClObj.put(area, new JSONObject());
                }
                else
                {
                    if (cClObj.has(area))
                        cClObj.put(area, new JSONObject());
                }
            });
            
            DATA_MAP.forEach((k, v) ->
            {
                String area = k.substring(0, 2);
                if (k.charAt(4) == ':')
                {
                    if (sClObj.has(area))
                        sClObj.getJSONObject(area).put(k, v);
                }
                else
                {
                    if (cClObj.has(area))
                        cClObj.getJSONObject(area).put(k, v);
                }
            });
            
            sClObj.keys().forEachRemaining(k ->
            {
                try(BufferedWriter bw = new BufferedWriter(new FileWriter(new File(TDDataDir, k+".s.td"))))
                {
                    sClObj.getJSONObject(k).write(bw);
                }
                catch (IOException ex) { NRODClient.printThrowable(ex, "TD"); }
            });
            cClObj.keys().forEachRemaining(k ->
            {
                try(BufferedWriter bw = new BufferedWriter(new FileWriter(new File(TDDataDir, k+".c.td"))))
                {
                    cClObj.getJSONObject(k).write(bw);
                }
                catch (IOException ex) { NRODClient.printThrowable(ex, "TD"); }
            });
        }
    }

    public long getTimeout() { return System.currentTimeMillis() - lastMessageTime; }
    public long getTimeoutThreshold() { return 30000; }

    private void printTD(String message, boolean toNew, long timestamp)
    {
        if (NRODClient.verbose)
        {
            //if (toErr)
            //    NRODClient.printErr("[TD] " + message);
            //else
                NRODClient.printOut("[TD] " + message);
        }

        String newDate = NRODClient.sdfDate.format(new Date());
        if (!lastLogDate.equals(newDate))
        {
            logNewStream.close();
            logOldStream.close();

            lastLogDate = newDate;

            logNewFile = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "TD" + File.separator + newDate.replace("/", "-") + ".log");
            logNewFile.getParentFile().mkdirs();
            try
            {
                logNewFile.createNewFile();
                logNewStream = new PrintWriter(new BufferedWriter(new FileWriter(logNewFile, true)), true);
            }
            catch (IOException e) { NRODClient.printThrowable(e, "TD"); }

            logOldFile = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "TDOld" + File.separator + newDate.replace("/", "-") + ".log");
            logOldFile.getParentFile().mkdirs();
            try
            {
                logOldFile.createNewFile();
                logOldStream = new PrintWriter(new BufferedWriter(new FileWriter(logOldFile, true)), true);
            }
            catch (IOException e) { NRODClient.printThrowable(e, "TD"); }

            File fileReplaySave = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "ReplaySaves" + File.separator + newDate.replace("/", "-") + ".json");
            fileReplaySave.getParentFile().mkdirs();
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileReplaySave)))
            {
                new JSONObject().put("TDData", DATA_MAP).write(bw);
            }
            catch (IOException e) { NRODClient.printThrowable(e, "TD"); }
        }
        
        (toNew ? logNewStream : logOldStream).println("[" + NRODClient.sdfDateTime.format(new Date(timestamp)) + "] " + message);

    }
}