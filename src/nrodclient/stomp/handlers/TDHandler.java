package nrodclient.stomp.handlers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import nrodclient.NRODClient;
import nrodclient.stomp.NRODListener;
import nrodclient.stomp.StompConnectionHandler;
import org.json.JSONArray;
import org.json.JSONObject;

public class TDHandler implements NRODListener
{
    private static PrintWriter logNewStream;
    private static File        logNewFile;
    private static String      lastLogDate = "";
    private        long        lastMessageTime = 0;

    private static NRODListener instance = null;
    private TDHandler()
    {
        lastLogDate = NRODClient.sdfDate.format(new Date());

        logNewFile = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "TD" + File.separator + lastLogDate.replace("/", "-") + ".log");
        logNewFile.getParentFile().mkdirs();
        try { logNewStream = new PrintWriter(new BufferedWriter(new FileWriter(logNewFile, true)), true); }
        catch (IOException e) { NRODClient.printThrowable(e, "TD"); }

        lastMessageTime = System.currentTimeMillis();
    }
    public static NRODListener getInstance()
    {
        if (instance == null)
            instance = new TDHandler();

        return instance;
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

                    String msgAddr = indvMsg.getString("area_id") + indvMsg.optString("address");

                    switch (msgType.toUpperCase())
                    {
                        case "CA_MSG":
                        {
                            updateMap.put(indvMsg.getString("area_id") + indvMsg.getString("from"), "");
                            updateMap.put(indvMsg.getString("area_id") + indvMsg.getString("to"), indvMsg.getString("descr"));

                            printTD(String.format("CA %s %s %s",
                                    indvMsg.getString("descr"),
                                    indvMsg.getString("area_id") + indvMsg.getString("from"),
                                    indvMsg.getString("area_id") + indvMsg.getString("to")),
                                    Long.parseLong(indvMsg.getString("time")));
                            break;
                        }

                        case "CB_MSG":
                        {
                            updateMap.put(indvMsg.getString("area_id") + indvMsg.getString("from"), "");

                            printTD(String.format("CB %s %s",
                                    indvMsg.getString("descr"),
                                    indvMsg.getString("area_id") + indvMsg.getString("from")),
                                    Long.parseLong(indvMsg.getString("time")));
                            break;
                        }

                        case "CC_MSG":
                        {
                            String to = indvMsg.getString("area_id") + indvMsg.getString("to");
                            updateMap.put(to, indvMsg.getString("descr"));

                            printTD(String.format("CC %s %s%s",
                                    indvMsg.getString("descr"),
                                    indvMsg.getString("area_id") + indvMsg.getString("to"),
                                    "".equals(DATA_MAP.getOrDefault(to, "")) ?
                                        "" : (" " + DATA_MAP.get(to))),
                                    Long.parseLong(indvMsg.getString("time")));
                            break;
                        }

                        case "CT_MSG":
                        {
                            updateMap.put("XXHB" + indvMsg.getString("area_id"), indvMsg.getString("report_time"));

                            printTD(String.format("CT %s %s",
                                    indvMsg.getString("area_id"),
                                    indvMsg.getString("report_time")),
                                    Long.parseLong(indvMsg.getString("time")));
                            break;
                        }

                        case "SF_MSG":
                        {
                            char[] data = zfill(Integer.toBinaryString(Integer.parseInt(indvMsg.getString("data"), 16)), 8).toCharArray();

                            for (int i = 0; i < data.length; i++)
                            {
                                String address = msgAddr + ":" + (8 - i);
                                String dataBit = String.valueOf(data[i]);

                                if (!DATA_MAP.containsKey(address) || DATA_MAP.get(address) == null || !dataBit.equals(DATA_MAP.get(address)))
                                {
                                    printTD(String.format("SF %s %s %s",
                                            address,
                                            DATA_MAP.getOrDefault(address, "0"),
                                            dataBit),
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
                                    if (!DATA_MAP.containsKey(id) || DATA_MAP.get(id) == null || !dat.equals(DATA_MAP.get(id)))
                                    {
                                        printTD(String.format("%s %s %s %s",
                                                indvMsg.get("msg_type"),
                                                id,
                                                DATA_MAP.getOrDefault(id, "0"),
                                                dat
                                            ),
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

    private static String zfill(String s, int len)
    {
        return String.format("%"+len+"s", s).replace(" ", "0");
    }

    private static void saveTDData(Map<String, String> mapToSave)
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
                    if (!sClObj.has(area))
                        sClObj.put(area, new JSONObject());
                }
                else
                {
                    if (!cClObj.has(area))
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

    private void printTD(String message, long timestamp)
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

            lastLogDate = newDate;

            logNewFile = new File(NRODClient.EASM_STORAGE_DIR, "Logs" + File.separator + "TD" + File.separator + newDate.replace("/", "-") + ".log");
            logNewFile.getParentFile().mkdirs();
            try
            {
                logNewFile.createNewFile();
                logNewStream = new PrintWriter(new BufferedWriter(new FileWriter(logNewFile, true)), true);
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

        logNewStream.println("[" + NRODClient.sdfDateTime.format(new Date(timestamp)) + "] " + message);
    }
}
