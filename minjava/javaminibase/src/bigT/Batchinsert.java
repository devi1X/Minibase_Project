package bigT;

import bigT.Map;
import bigT.bigT;
import btree.BTFileScan;
import btree.BTreeFile;
import global.GlobalConst;
import global.MID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.*;
import java.util.Vector;



public class Batchinsert{
    private boolean OK = true;
    private boolean FAIL = false;


    private bigT bigtable;
    private BTreeFile bTreeFile;
    private BTFileScan btScan,btScan1;
//    private Map t;

    private String fileName;
    private String tableName;
    private int tableType;
    private int NUMBUF;

    public int insertCount = 0;

    public Batchinsert(){
        System.out.println("Please enter the table name: ");
        tableName = getCommand();

        System.out.println("Please enter the table type(1,2 or 4): ");
        tableType = Integer.parseInt(getCommand());

        System.out.println("Please enter the NUMBUF: ");
        NUMBUF = Integer.parseInt(getCommand());
    }

    public Batchinsert(String dataFileName, int type, String bigTName, int noBuffer) {
        fileName = dataFileName;
        tableType = type;
        tableName = bigTName;
        NUMBUF = noBuffer;
    }

    public bigT runInsertTest() throws HFDiskMgrException, HFException, HFBufMgrException, IOException {

        boolean status = OK;

        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.testdb";
        SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock" );

        bigtable = new bigT(tableName);
        File inputFile = new File(this.fileName);
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.replaceAll("[^\\x00-\\x7F]", "");

                String[] fields = line.split(",");
                Map map = new Map();
                map.setDefaultHdr();
                String rl = fields[0];
                String cl = fields[1];
                int ts = Integer.parseInt(fields[2]);
                String val = fields[3];
                map.setRowLabel(rl);
                map.setColumnLabel(cl);
                map.setTimeStamp(ts);
                map.setValue(val);
                bigtable.insertMap(map, tableType);
                insertCount += 1;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            throw new RuntimeException(e);
        } catch (HFException e) {
            throw new RuntimeException(e);
        } catch (HFBufMgrException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (status != OK) {
            //bail out
            System.err.println ("*** Error creating relation for sailors");
            Runtime.getRuntime().exit(1);
        }


        try {
            int mapcount = bigtable.getMapCnt();
            System.out.println ("There are " + mapcount + " maps");
            //System.out.println(bigtable.heapfile);
        }
        catch (Exception e) {
            System.err.println("*** error in Map.getMapCnt() ***");
            status = FAIL;
            e.printStackTrace();
        }

        return bigtable;
    }

    public String getCommand(){
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        String s = null;

        try {
            s = in.readLine();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return s;
    }
}