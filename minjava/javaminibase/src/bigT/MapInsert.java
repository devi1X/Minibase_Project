package bigT;

import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;

import java.io.IOException;

public class MapInsert {
    private String rowLabel;
    private String colLabel;
    private String value;
    private String timeStamp;
    private String tableName;
    private int numBuff;

    public MapInsert(String rl, String cl, String val, String ts, String tableName, int numBuff) {
        rowLabel = rl;
        colLabel = cl;
        value = val;
        timeStamp = ts;
        this.tableName = tableName;
        this.numBuff = numBuff;
    }
    public void runInsert() throws IOException, InvalidTupleSizeException, InvalidTypeException {
        Map map = new Map();
        map.setHdr((short) 4,map.getAttrType(), map.getMapSizes());
        map.setRowLabel(rowLabel);
        map.setColumnLabel(colLabel);
        map.setTimeStamp(Integer.parseInt(timeStamp));
        map.setValue(value);



    }
}
