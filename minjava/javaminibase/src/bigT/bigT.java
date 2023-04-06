package bigT;


import global.*;
import heap.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import diskmgr.*;
import bufmgr.*;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.MapUtils;
import iterator.RelSpec;


public class bigT{

    public PageId _firstDirPageId;
    String tableName;
    int orderType;
    //Stream stream;
    Heapfile heapfile;

    HashMap<String, ArrayList<MID>> tempMap;

    //这里需求理解错了，"between1 and 5 and the different types will correspond to different clustering and indexing strategies you
    //will use for the bigtable." Type也是index/Cluster类型 Let me do it.
    public bigT(java.lang.String name, int type) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {

        tableName = name;
        orderType = type;
        heapfile = new Heapfile(name);
    }
    //complete
    public void deleteBigt() throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, FileAlreadyDeletedException, InvalidMapSizeException {
        this.heapfile.deleteFile();

    }
    //complete
    public int getMapCnt() throws HFBufMgrException, IOException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, InvalidMapSizeException {

        int total_Map = this.heapfile.getRecCnt();
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
    //complete
    public MID insertMap(byte[] mapPtr) throws Exception {
        //removeOldMap(mapPtr, heapfile);

        MID mid = this.heapfile.insertRecord(mapPtr);
        MID resultMID = new MID(mid.pageNo, mid.slotNo);
        return resultMID;
    }

    //complete
    public Stream openStream(int orderType, java.lang.String rowFilter, java.lang.String columnFilter, java.lang.String valueFilter) throws Exception {
        return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
    }

    public String getBigtName() {
        return this.tableName;
    }

    public int getBigtType() {
        return this.orderType;
    }


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