package bigT;

import bigT.Map;
import bigT.bigT;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.StringKey;
import global.GlobalConst;
import global.MID;
import global.SystemDefs;
import heap.*;

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
    //public  Vector<MID> mids = new Vector<MID>();
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

    public bigT runInsertTest() throws Exception {

        boolean status = OK;

        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.testdb";
        SystemDefs sysdef = new SystemDefs(dbpath, 9000, NUMBUF, "Clock" );

        bigtable = new bigT(tableName);
        File inputFile = new File(this.fileName);
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.replaceAll("[^\\x00-\\x7F]", "");

                String[] fields = line.split(",");
                Map map = new Map();
                map.setDefaultHdr();
//                String rl = fields[0];
//                String cl = fields[1];
//                int ts = Integer.parseInt(fields[2]);
//                String val = fields[3];
//                map.setRowLabel(rl);
//                map.setColumnLabel(cl);
//                map.setTimeStamp(ts);
//                map.setValue(val);

//                SSS used fields[2] for value and fields[3] for timestamp
//                I think it should be the other way, so I swapped it below
//                But his method worked. So there might be a reason
//                Just leave a note here for future reference... Meng
                map.setRowLabel(fields[0]);
                map.setColumnLabel(fields[1]);
                map.setTimeStamp(Integer.parseInt(fields[2]));
                String valueLabel = fields[3];
                for(int j=fields[3].length(); j < Map.DEFAULT_STRING_ATTRIBUTE_SIZE; j++){
                    valueLabel = "0"+valueLabel;
                }
                map.setValue(valueLabel);
                MID temp = bigtable.insertMap(map, tableType);
                bigtable.mids.add(temp);
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

//        get the heapfile that's corresponding to the type, add indexfile into index map
//        index minus one because the file name starts from 1
//        This can be made into another function. But for now it stays like this for convenience
        if (tableType != 1) {
            Heapfile heapfile = bigtable.heapFiles.get(tableType - 1);
            Stream stream = new Stream(heapfile);
            MID mid = stream.getNextMID();
            BTreeFile indexFile = null;
            while (mid != null) {
                Map map = heapfile.getRecord(mid);
                StringKey key = null;
                switch (tableType) {
                    case 2:
                        key = new StringKey(map.getRowLabel());
                        break;
                    case 3:
                        key = new StringKey(map.getColumnLabel());
                        break;
                    case 4:
                        key = new StringKey(map.getRowLabel() + '%' + map.getRowLabel());
                        break;
                    case 5:
                        key = new StringKey(map.getRowLabel() + '%' + map.getValue());
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid table type");
                }
                indexFile = bigtable.indexFiles.get(tableType - 1);
                indexFile.insert(key, mid);

                mid = stream.getNextMID();
            }
            stream.closestream();
        }
        try {
            int mapcount = bigtable.getMapCnt();
            System.out.println("There are " + mapcount + " maps");
            //System.out.println(bigtable.heapfile);
        } catch (Exception e) {
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