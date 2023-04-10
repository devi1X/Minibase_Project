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
    private Map t;

    private String fileName;
    private String tableName;
    private int tableType;
    private int NUMBUF;

    public int insertCount = 0;

    public Batchinsert(){
        //System.out.println("Please enter the fileName: ");
        // fileName = getCommand();

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
//    public void runBatchInsert() {
////        SystemDef stuff, copied from the Sailor test section
//        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.testdb";
//        String logpath = "/tmp/"+System.getProperty("user.name")+".testlog";
//
//        String remove_cmd = "/bin/rm -rf ";
//        String remove_logcmd = remove_cmd + logpath;
//        String remove_dbcmd = remove_cmd + dbpath;
//        String remove_joincmd = remove_cmd + dbpath;
//
//        try {
//            Runtime.getRuntime().exec(remove_logcmd);
//            Runtime.getRuntime().exec(remove_dbcmd);
//            Runtime.getRuntime().exec(remove_joincmd);
//        }
//        catch (IOException e) {
//            System.err.println (""+e);
//        }
//        SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock" );
//
////Read file by each row, convert to map and insert into corresponding heapfile
//        File inputFile = new File(this.fileName);
//        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
//            bigT table = new bigT(tableName);
//            String line;
//            while ((line = br.readLine()) != null) {
//                byte[] mapPtr = line.getBytes();
//                Map map = new Map(mapPtr, 0);
//                //System.out.println("insert in batchinsert before");
//                table.insertMap(map, tableType);
//                //System.out.println("insert in batchinsert after");
//                insertCount += 1;
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (HFDiskMgrException e) {
//            throw new RuntimeException(e);
//        } catch (HFException e) {
//            throw new RuntimeException(e);
//        } catch (HFBufMgrException e) {
//            throw new RuntimeException(e);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }

    public bigT runInsertTest() throws HFDiskMgrException, HFException, HFBufMgrException, IOException {

        boolean status = OK;
        int numsailors = 17;
        int numsailors_attrs = 4;

        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.testdb";
        String logpath = "/tmp/"+System.getProperty("user.name")+".testlog";

        String remove_cmd = "/bin/rm -rf ";
        String remove_logcmd = remove_cmd + logpath;
        String remove_dbcmd = remove_cmd + dbpath;
        String remove_joincmd = remove_cmd + dbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
            Runtime.getRuntime().exec(remove_joincmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock" );

        t = new Map();
        try {
            t.setHdr((short) 4,t.getAttrType(), t.getMapSizes());
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // inserting the tuple into file "sailors"
        MID mid;
        bigtable = null;
        try {
            bigtable = new bigT(tableName);
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Map(size);
        try {
            t.setHdr((short) 4, t.getAttrType(), t.getMapSizes());
        }
        catch (Exception e) {
            System.err.println("*** error in Map.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        File inputFile = new File(this.fileName);
        bigT table = new bigT(tableName);
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {

            String line;
            String[] labels;
            Map map = new Map();

            while ((line = br.readLine()) != null) {
                //System.out.println(line);
                labels = line.split(",");
                map.setHdr((short) 4, map.getAttrType(), map.getMapSizes());
                map.setRowLabel(labels[0]);
                map.setColumnLabel(labels[1]);
                map.setTimeStamp(Integer.parseInt(labels[3]));
                //map.setMapLength(line.length());
                String valueLabel = labels[2];
                for(int j=labels[2].length(); j < Map.DEFAULT_STRING_ATTRIBUTE_SIZE; j++){
                    valueLabel = "0"+valueLabel;
                }
                map.setValue(valueLabel);
                //map.print();
                //byte[] mapPtr = line.getBytes();
                //Map map = new Map(mapPtr, 0);
                table.insertMap(map, tableType);
                //map.getMapByteArray();

                //System.out.println("insert in batchinsert after");
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
//        for (int i=0; i<numsailors; i++) {
//            try {
//                t.setStrFld(1, ((Sailor)sailors.elementAt(i)).sid);
//                t.setStrFld(2, ((Sailor)sailors.elementAt(i)).sname);
//                t.setIntFld(3, ((Sailor)sailors.elementAt(i)).rating);
//                t.setStrFld(4, ((Sailor)sailors.elementAt(i)).age);
//            }
//            catch (Exception e) {
//                System.err.println("*** bigt error in Map.setStrFld() ***");
//                status = FAIL;
//                e.printStackTrace();
//            }
//
////            try {
////                System.out.println(t.getMapSizes());
////                mid = bigtable.insertMap(t.returnMapByteArray());
////                System.out.println(mid);
////                String columLable = t.getColumnLabel();
////                System.out.println("Column " + i + " Label: " + columLable);
////            }
////            catch (Exception e) {
////                System.err.println("*** error in map.insertMap() ***");
////                status = FAIL;
////                e.printStackTrace();
////            }
//
//
//
//
//        }
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