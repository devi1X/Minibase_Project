package bigT;


import global.*;
import heap.*;
import java.io.*;
import java.util.*;

import diskmgr.*;
import bufmgr.*;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.MapUtils;
import iterator.RelSpec;


public class bigT{

    public PageId _firstDirPageId;
    String tableName;
//    orderType not needed in phase 3 - Meng
//    int orderType;
    //Stream stream;
    public Heapfile heapfile;
    public Vector<MID> mids = new Vector<MID>();
    private ArrayList<Heapfile> heapFiles;

    HashMap<String, ArrayList<MID>> tempMap;

    //这里需求理解错了，"between1 and 5 and the different types will correspond to different clustering and indexing strategies you
    //will use for the bigtable." Type也是index/Cluster类型 Let me do it.
    public bigT(java.lang.String name) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {

        tableName = name;
//        orderType = type;
//        heapfile = new Heapfile(name);
//        initialize 5 heapfiles, each with the name of i (0,1,2,3,4)
        heapFiles = new ArrayList<Heapfile>();
        for (int i = 0; i < 5; i++) {
            String fileName = name + i;
            Heapfile file = new Heapfile(fileName);
            heapFiles.add(file);
        }
    }
    //complete
    public void deleteBigt() throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, FileAlreadyDeletedException, InvalidMapSizeException {
        for (int i = 0; i < 5; i++) {
            this.heapFiles.get(i).deleteFile();
        }

    }
    //complete
    public int getMapCnt() throws HFBufMgrException, IOException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, InvalidMapSizeException {

        int total_Map = 0;
        for (int i = 0; i < 5; i++) {
            total_Map += this.heapFiles.get(i).getRecCnt();
        }
        return total_Map;

    }



    public int getRowCnt() throws Exception {
        Set<String> rowCnt = new HashSet<>();
        tempMap.keySet().forEach(key -> rowCnt.add(key.split("\\$")[0]));
        return rowCnt.size();

    }

    public int getColumnCnt() throws IOException {
        Set<String> colCnt = new HashSet<>();
        tempMap.keySet().forEach(key -> colCnt.add(key.split("\\$")[1]));
        return colCnt.size();

    }
    //This can be deleted once the Sailor part is removed? - Meng
    public MID insertMap(byte[] mapPtr) throws Exception {
        //removeOldMap(mapPtr, heapfile);
        System.out.println("start insert");
        MID mid = this.heapfile.insertRecord(mapPtr);
        MID resultMID = new MID(mid.pageNo, mid.slotNo);
        System.out.println("end insert");
        return resultMID;
    }

//    New constructor that takes map and storage type. Need to implement the limit 3 records function - Meng
    public MID insertMap(Map map, int type) throws Exception {
        byte[] mapPtr = map.getMapByteArray();
        Heapfile hf = heapFiles.get(type);
        MID mid = hf.insertRecord(mapPtr);
//        MID result_mid = new MID(mid.pageNo, mid.slotNo);
//
        mids.add(mid);
//        //System.out.println(result_mid);
//        return result_mid;
        return mid;
    }

    //complete
    public Stream openStream(int orderType, java.lang.String rowFilter, java.lang.String columnFilter, java.lang.String valueFilter) throws Exception {
        return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
    }

    public String getBigtName() {
        return this.tableName;
    }
    public Map getMap(int index) throws Exception {
        System.out.println(mids.elementAt(index));
        Map map = this.heapfile.getRecord(mids.elementAt(index));

        return map;
    }

//    public int getBigtType() {
//        return this.orderType;
//    }


//    public static void removeOldMap(byte[] mapPtr, Heapfile heapfile) throws Exception {
//        Stream mapStream = new Stream(heapfile);
//        MID currMapMID = mapStream.getFirstMap();
//        MID[] matchingMaps = new MID[2];
//        short matchingMapCount = 0;
//
//        while (currMapMID != null) {
//            byte[] currMapPtr = heapfile.getRecord(currMapMID).getMapByteArray();
//            if (currMapPtr[0] == mapPtr[0] && currMapPtr[1] == mapPtr[1]) {
//                switch (matchingMapCount) {
//                    case 0:
//                        matchingMaps[0] = currMapMID;
//                        matchingMapCount ++;
//                    case 1:
//                        matchingMaps[1] = currMapMID;
//                        matchingMapCount ++;
//                    case 2:
//                        heapfile.deleteRecord(matchingMaps[0]);
//                        MID[] newMatchingMaps = {matchingMaps[1], currMapMID};
//                        matchingMaps = newMatchingMaps;
//                        break;
//                }
//                currMapMID = mapStream.getNext();
//            }
//        }
//    }



}