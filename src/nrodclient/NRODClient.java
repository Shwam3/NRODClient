package nrodclient;

import java.awt.AWTException;
import java.awt.CheckboxMenuItem;
import java.awt.Desktop;
import java.awt.EventQueue;
import java.awt.Menu;
import java.awt.MenuItem;
import java.awt.MouseInfo;
import java.awt.Point;
import java.awt.PopupMenu;
import java.awt.Robot;
import java.awt.SystemTray;
import java.awt.Toolkit;
import java.awt.TrayIcon;
import java.awt.event.ActionEvent;
import java.awt.event.ItemEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import javax.swing.JOptionPane;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import nrodclient.stomp.StompConnectionHandler;
import nrodclient.stomp.handlers.MVTHandler;
import nrodclient.stomp.handlers.RTPPMHandler;
import nrodclient.stomp.handlers.TDHandler;
import nrodclient.stomp.handlers.TSRHandler;
import nrodclient.stomp.handlers.VSTPHandler;
import org.json.JSONException;
import org.json.JSONObject;

public class NRODClient
{
    public static final String VERSION = "2";

    public static final boolean verbose = false;
    
    public static final File EASM_STORAGE_DIR = new File(System.getProperty("user.home", "C:") + File.separator + ".easigmap");
    public static JSONObject config = new JSONObject();

    public static SimpleDateFormat sdfTime          = new SimpleDateFormat("HH:mm:ss");
    public static SimpleDateFormat sdfDate          = new SimpleDateFormat("dd/MM/yy");
    public static SimpleDateFormat sdfDateTime      = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
    public static SimpleDateFormat sdfDateTimeShort = new SimpleDateFormat("dd/MM HH:mm:ss");

    public  static PrintStream  logStream;
    private static File         logFile;
    private static String       lastLogDate = "";

    private static TrayIcon sysTrayIcon = null;
    public  static DataGui  guiData;

    public static PrintStream stdOut = System.out;
    public static PrintStream stdErr = System.err;
    
    public static void main(String[] args) throws IOException, GeneralSecurityException
    {
        try { UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName()); }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | UnsupportedLookAndFeelException e) { printThrowable(e, "Look & Feel"); }

        Date logDate = new Date();
        logFile = new File(EASM_STORAGE_DIR, "Logs" + File.separator + "NRODClient" + File.separator + sdfDate.format(logDate).replace("/", "-") + ".log");
        logFile.getParentFile().mkdirs();
        lastLogDate = sdfDate.format(logDate);

        try
        {
            logStream = new PrintStream(new FileOutputStream(logFile, logFile.length() > 0), true);
            System.setOut(logStream);
            System.setErr(logStream);
        }
        catch (FileNotFoundException e) { printErr("Could not create log file"); printThrowable(e, "Startup"); }
        
        reloadConfig();
        
        try { EventQueue.invokeAndWait(() -> guiData = new DataGui()); }
        catch (InvocationTargetException | InterruptedException e) { printThrowable(e, "Startup"); }
        
        try
        {
            File TDDataDir = new File(NRODClient.EASM_STORAGE_DIR, "TDData");
            Arrays.stream(TDDataDir.listFiles()).forEach(f ->
            {
                if (f.isFile() && f.getName().endsWith(".td"))
                {
                    String dataStr = "";
                    try(BufferedReader br = new BufferedReader(new FileReader(f)))
                    {
                        dataStr = br.readLine();
                    }
                    catch (IOException ex) { printThrowable(ex, "TD-Startup"); }
                    
                    try
                    {
                        JSONObject data = new JSONObject(dataStr);
                        data.keys().forEachRemaining(k -> TDHandler.DATA_MAP.putIfAbsent(k, data.getString(k)));
                    }
                    catch (JSONException e) { NRODClient.printErr("[TD-Startup] Malformed JSON in " + f.getName()); }
                }
            });
        }
        catch (Exception e) { NRODClient.printThrowable(e, "TD-Startup"); }
        
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            printOut("[Main] Stopping...");
            
            StompConnectionHandler.disconnect();
        }, "NRODShutdown"));
        
        if (StompConnectionHandler.wrappedConnect())
            StompConnectionHandler.printStomp("Initialised and working", false);
        else
            StompConnectionHandler.printStomp("Unble to start", true);

        Timer sleepTimer = new Timer("sleepTimer", true);
        sleepTimer.scheduleAtFixedRate(new TimerTask()
        {
            @Override
            public void run()
            {
                try
                {
                    Point mouseLoc = MouseInfo.getPointerInfo().getLocation();
                    new Robot().mouseMove(mouseLoc.x, mouseLoc.y);
                }
                catch (NullPointerException e) {}
                catch (Exception e) { printErr("[Timer] Exception: " + e.toString()); }
            }
        }, 30000, 30000);

        updatePopupMenu();
    }

    //<editor-fold defaultstate="collapsed" desc="Print methods">
    public static void printThrowable(Throwable t, String name)
    {
        name = name == null ? "" : name;
        
        StackTraceElement caller = Thread.currentThread().getStackTrace()[2];
        name += (name.isEmpty() ? "" : " ");
        name += caller.getFileName() != null && caller.getLineNumber() >= 0 ?
          "(" + caller.getFileName() + ":" + caller.getLineNumber() + ")" :
          (caller.getFileName() != null ?  "("+caller.getFileName()+")" : "(Unknown Source)");
            
        printErr("[" + name + "] " + t.toString());

        for (StackTraceElement element : t.getStackTrace())
            printErr("[" + name + "] -> " + element.toString());

        for (Throwable sup : t.getSuppressed())
            printThrowable0(sup, name);

        printThrowable0(t.getCause(), name);
    }

    private static void printThrowable0(Throwable t, String name)
    {
        if (t != null)
        {
            printErr((name != null && !name.isEmpty() ? "[" + name + "] " : "") + t.toString());

            for (StackTraceElement element : t.getStackTrace())
                printErr((name != null && !name.isEmpty() ? "[" + name + "] -> " : " -> ") + element.toString());
        }
    }

    public static void printOut(String message)
    {
        if (message != null && !message.equals(""))
            if (!message.contains("\n"))
                print("[" + sdfDateTime.format(new Date()) + "] " + message, false);
            else
                for (String msgPart : message.split("\n"))
                    print("[" + sdfDateTime.format(new Date()) + "] " + msgPart, false);
    }

    public static void printErr(String message)
    {
        if (message != null && !message.equals(""))
            if (!message.contains("\n"))
                print("[" + sdfDateTime.format(new Date()) + "] !!!> " + message + " <!!!", false);
            else
                for (String msgPart : message.split("\n"))
                    print("[" + sdfDateTime.format(new Date()) + "] !!!> " + msgPart + " <!!!", true);
    }

    private static synchronized void print(String message, boolean toErr)
    {
        if (toErr)
            stdErr.println(message);
        else
            stdOut.println(message);

        filePrint(message);
    }

    private static synchronized void filePrint(String message)
    {
        Date logDate = new Date();
        if (!lastLogDate.equals(sdfDate.format(logDate)))
        {
            logStream.flush();
            logStream.close();

            lastLogDate = sdfDate.format(logDate);

            logFile = new File(EASM_STORAGE_DIR, "Logs" + File.separator + "NRODClient" + File.separator + lastLogDate.replace("/", "-") + ".log");
            logFile.getParentFile().mkdirs();

            try
            {
                logFile.createNewFile();
                logStream = new PrintStream(new FileOutputStream(logFile, true));

                System.setOut(logStream);
                System.setErr(logStream);
            }
            catch (IOException e) { printErr("Could not create log file"); printThrowable(e, "Logging"); }
        }

        logStream.println(message);
    }
    //</editor-fold>

    public static synchronized void updatePopupMenu()
    {
        if (SystemTray.isSupported())
        {
            try
            {
                PopupMenu menu             = new PopupMenu();
                MenuItem itemExit          = new MenuItem("Exit");
                MenuItem itemOpenLog       = new MenuItem("Open Log File");
                MenuItem itemOpenLogFolder = new MenuItem("Open Log File Folder");
                MenuItem itemStatus        = new MenuItem("Status...");
                MenuItem itemData          = new MenuItem("View Data...");
                MenuItem itemRTPPMUpload   = new MenuItem("Upload RTPPM file");
                MenuItem itemReconnect     = new MenuItem("Stomp Reconnect");

                Menu menuSubscriptions                  = new Menu("Subscriptions");
                CheckboxMenuItem itemSubscriptionsRTPPM = new CheckboxMenuItem("RTPPM", StompConnectionHandler.isSubscribedRTPPM());
                CheckboxMenuItem itemSubscriptionsMVT   = new CheckboxMenuItem("MVT",   StompConnectionHandler.isSubscribedMVT());
                CheckboxMenuItem itemSubscriptionsVSTP  = new CheckboxMenuItem("VSTP",  StompConnectionHandler.isSubscribedVSTP());
                CheckboxMenuItem itemSubscriptionsTSR   = new CheckboxMenuItem("TSR",   StompConnectionHandler.isSubscribedTSR());

                itemStatus.addActionListener((ActionEvent e) ->
                {
                    NRODClient.updatePopupMenu();
                    
                    StringBuilder statusMsg = new StringBuilder();
                    statusMsg.append("\nStomp:");
                    statusMsg.append("\n  Connection: ").append(StompConnectionHandler.isConnected() ? "Connected" : "Disconnected").append(StompConnectionHandler.isTimedOut() ? " (timed out)" : "");
                    statusMsg.append("\n  Timeout: ").append((System.currentTimeMillis() - StompConnectionHandler.lastMessageTimeGeneral) / 1000f).append("s");
                    statusMsg.append("\n  Subscriptions:");
                    statusMsg.append("\n    TD: ").append(String.format("%s - %.2fs", StompConnectionHandler.isSubscribedTD() ? "Yes" : "No", TDHandler.getInstance().getTimeout() / 1000f));
                    statusMsg.append("\n    MVT: ").append(String.format("%s - %.2fs", StompConnectionHandler.isSubscribedMVT() ? "Yes" : "No", MVTHandler.getInstance().getTimeout() / 1000f));
                    statusMsg.append("\n    RTPPM: ").append(String.format("%s - %.2fs", StompConnectionHandler.isSubscribedRTPPM() ? "Yes" : "No", RTPPMHandler.getInstance().getTimeout() / 1000f));
                    statusMsg.append("\n    VSTP: ").append(String.format("%s - %.2fs", StompConnectionHandler.isSubscribedVSTP() ? "Yes" : "No", VSTPHandler.getInstance().getTimeout() / 1000f));
                    statusMsg.append("\n    TSR: ").append(String.format("%s - %.2fs", StompConnectionHandler.isSubscribedTSR() ? "Yes" : "No", TSRHandler.getInstance().getTimeout() / 1000f));
                    statusMsg.append("\nLogfile: \"").append(NRODClient.logFile.getName()).append("\"");
                    statusMsg.append("\nStarted: ").append(NRODClient.sdfDateTime.format(ManagementFactory.getRuntimeMXBean().getStartTime()));
                    
                    JOptionPane.showMessageDialog(null, statusMsg.toString(), "NRODClient - Status", JOptionPane.INFORMATION_MESSAGE);
                });
                itemData.addActionListener(e -> NRODClient.guiData.setVisible(true));
                itemRTPPMUpload.addActionListener((ActionEvent e) ->
                {
                    RTPPMHandler.uploadHTML();
                });
                itemReconnect.addActionListener((ActionEvent e) ->
                {
                    if (JOptionPane.showConfirmDialog(null, "Are you sure you wish to reconnect?", "Confirmation", JOptionPane.OK_CANCEL_OPTION) == JOptionPane.OK_OPTION)
                    {
                        StompConnectionHandler.disconnect();
                        StompConnectionHandler.wrappedConnect();
                    }
                });
                itemSubscriptionsRTPPM.addItemListener((ItemEvent e) -> { StompConnectionHandler.toggleRTPPM(); });
                itemSubscriptionsMVT  .addItemListener((ItemEvent e) -> { StompConnectionHandler.toggleMVT(); });
                itemSubscriptionsVSTP .addItemListener((ItemEvent e) -> { StompConnectionHandler.toggleVSTP(); });
                itemSubscriptionsTSR  .addItemListener((ItemEvent e) -> { StompConnectionHandler.toggleTSR(); });
                itemOpenLog.addActionListener((ActionEvent evt) ->
                {
                    try { Desktop.getDesktop().open(NRODClient.logFile); }
                    catch (IOException e) {}
                });
                itemOpenLogFolder.addActionListener((ActionEvent evt) ->
                {
                    try { Runtime.getRuntime().exec("explorer.exe /select," + NRODClient.logFile); }
                    catch (IOException e) {}
                });
                itemExit.addActionListener((ActionEvent e) ->
                {
                    if (JOptionPane.showConfirmDialog(null, "Are you sure you wish to exit?", "Confirmation", JOptionPane.OK_CANCEL_OPTION) == JOptionPane.OK_OPTION)
                    {
                        NRODClient.printOut("[Main] Stopping");
                        System.exit(0);
                    }
                });

                menuSubscriptions.add(itemSubscriptionsRTPPM);
                menuSubscriptions.add(itemSubscriptionsMVT);
                menuSubscriptions.add(itemSubscriptionsVSTP);
                menuSubscriptions.add(itemSubscriptionsTSR);

                menu.add(itemStatus);
                menu.add(itemData);
                menu.add(itemRTPPMUpload);
                menu.add(itemReconnect);
                menu.add(menuSubscriptions);
                menu.addSeparator();
                menu.add(itemOpenLog);
                menu.add(itemOpenLogFolder);
                menu.addSeparator();
                menu.add(itemExit);

                if (sysTrayIcon == null)
                {
                    sysTrayIcon = new TrayIcon(Toolkit.getDefaultToolkit().getImage(NRODClient.class.getResource("/nrodclient/resources/TrayIcon.png")), "NROD Client", menu);
                    sysTrayIcon.setImageAutoSize(true);
                    sysTrayIcon.addMouseListener(new MouseAdapter()
                    {
                        @Override
                        public void mouseClicked(MouseEvent e)
                        {
                            updatePopupMenu();
                        }
                    });
                }

                sysTrayIcon.setPopupMenu(menu);

                if (!Arrays.asList(SystemTray.getSystemTray().getTrayIcons()).contains(sysTrayIcon))
                    SystemTray.getSystemTray().add(sysTrayIcon);
            }
            catch (AWTException e) { printThrowable(e, "SystemTrayIcon"); }
        }
    }

    public static void reloadConfig()
    {
        try (BufferedReader br = new BufferedReader(new FileReader(new File(EASM_STORAGE_DIR, "config.json"))))
        {
            StringBuilder sb = new StringBuilder();
            
            String line;
            while ((line = br.readLine()) != null)
                sb.append(line);
            
            JSONObject obj = new JSONObject(sb.toString());
            config = obj;
        } catch (IOException | JSONException e) { NRODClient.printThrowable(e, "Config"); }
        
        List<String> filter = new ArrayList<>();
        config.getJSONArray("TD_Area_Filter").forEach(e -> filter.add((String) e));
        filter.sort(null);
        TDHandler.setAreaFilter(filter);
    }
}
