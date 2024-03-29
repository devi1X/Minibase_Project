package bigT;


import global.AttrType;
import global.Convert;
import global.GlobalConst;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidMapSizeException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;

import java.io.IOException;

public class Map implements GlobalConst {

    public static final short NUM_FIELDS = 4;
    public static final int MAX_SIZE = MINIBASE_PAGESIZE;
    public static final int DEFAULT_STRING_ATTRIBUTE_SIZE = 20;
    private static final short ROW_NUMBER = 1;
    private static final short COLUMN_NUMBER = 2;
    private static final short TIMESTAMP_NUMBER = 3;
    private static final short VALUE_NUMBER = 4;
    private byte[] data;
    private int mapOffset;
    private int mapLength;
    private short fieldCount;
    private short[] fieldOffset;

    /**
     * Default Map constructor to initialise a new map.
     */
    public Map() {
        this.data = new byte[MAX_SIZE];
        this.mapOffset = 0;
        this.mapLength = MAX_SIZE;
    }

    /**
     * @param amap Initialise map based on bytearray from given map.
     * @param offset map offset.
     * @throws IOException throws IO exception
     */
    public Map(byte[] amap, int offset) throws IOException {
        this.data = amap;
        this.mapOffset = offset;
        setFieldOffsetFromData();
        setFieldCount(Convert.getShortValue(offset, this.data));
    }

    /**
     * @param amap Initialise map based on bytearray from given map.
     * @param offset map offset.
     * @param mapLength Set length of the map.
     * @throws IOException throws IO Exception.
     */
    public Map(byte[] amap, int offset, int mapLength) throws IOException {
        this.data = amap;
        this.mapOffset = offset;
        this.mapLength = mapLength;
        setFieldOffsetFromData();
        setFieldCount(Convert.getShortValue(offset, this.data));
    }

    /**
     * @param fromMap Initialse maps given map object.
     */
    public Map(Map fromMap) {
        this.data = fromMap.getMapByteArray();
        this.mapLength = fromMap.getMapLength();
        this.mapOffset = 0;
        this.fieldCount = fromMap.getFieldCount();
        this.fieldOffset = fromMap.copyFieldOffset();

    }

    /**
     * @param size Initialze map based on given size.
     */
    public Map(int size) {
        this.data = new byte[size];
        this.mapOffset = 0;
        this.mapLength = size;
        this.fieldCount = 4;
    }


    public byte[] returnMapByteArray() {
        return data;
    }

    public void setData(byte[] data) throws IOException {
        this.data = data;
        setFieldOffsetFromData();
        setFieldCount(Convert.getShortValue(0, data));
    }

    public int getMapOffset() {
        return mapOffset;
    }

    public void setMapOffset(int mapOffset) {
        this.mapOffset = mapOffset;
    }

    public int getMapLength() {
        return mapLength;
    }

    public void setMapLength(int mapLength) {
        this.mapLength = mapLength;
    }

    public short getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(short fieldCount) {
        this.fieldCount = fieldCount;
    }

    public short[] getFieldOffset() {
        return fieldOffset;
    }

    public void setFieldOffset(short[] fieldOffset) {
        this.fieldOffset = fieldOffset;
    }

    public String getStringField(short fieldNumber) throws IOException, FieldNumberOutOfBoundException {
        if (fieldNumber == 3) {
            throw new FieldNumberOutOfBoundException(null, "MAP: INVALID_FIELD PASSED");
        } else {
            return Convert.getStrValue(this.fieldOffset[fieldNumber - 1], this.data, this.fieldOffset[fieldNumber] - this.fieldOffset[fieldNumber - 1]);
        }
    }

    public short[] copyFieldOffset() {
        short[] newFieldOffset = new short[this.fieldCount + 1];
        System.arraycopy(this.fieldOffset, 0, newFieldOffset, 0, this.fieldCount + 1);
        return newFieldOffset;
    }

    /**
     * @param fromMap Copy the map object to this map object.
     */
    public void copyMap(Map fromMap) {
        byte[] tempArray = fromMap.getMapByteArray();
        System.arraycopy(tempArray, 0, data, mapOffset, mapLength);
    }

    public String getRowLabel() throws IOException {
        return Convert.getStrValue(this.fieldOffset[ROW_NUMBER - 1], this.data, this.fieldOffset[ROW_NUMBER] - this.fieldOffset[ROW_NUMBER - 1]);
    }

    public void setRowLabel(String rowLabel) throws IOException {
        Convert.setStrValue(rowLabel, this.fieldOffset[ROW_NUMBER - 1], this.data);
    }

    public String getColumnLabel() throws IOException {
        return Convert.getStrValue(this.fieldOffset[COLUMN_NUMBER - 1], this.data, this.fieldOffset[COLUMN_NUMBER] - this.fieldOffset[COLUMN_NUMBER - 1]);
    }

    public void setColumnLabel(String columnLabel) throws IOException {
        Convert.setStrValue(columnLabel, this.fieldOffset[COLUMN_NUMBER - 1], this.data);
    }

    public int getTimeStamp() throws IOException {
        return Convert.getIntValue(this.fieldOffset[TIMESTAMP_NUMBER - 1], this.data);
    }

    public void setTimeStamp(int timeStamp) throws IOException {
        Convert.setIntValue(timeStamp, this.fieldOffset[TIMESTAMP_NUMBER - 1], this.data);
    }

    public String getValue() throws IOException {
        return Convert.getStrValue(this.fieldOffset[VALUE_NUMBER - 1], this.data, this.fieldOffset[VALUE_NUMBER] - this.fieldOffset[VALUE_NUMBER - 1]);
    }

    public void setValue(String value) throws IOException {
        Convert.setStrValue(value, this.fieldOffset[VALUE_NUMBER - 1], this.data);
    }

    public byte[] getMapByteArray() {
        byte[] mapCopy = new byte[this.mapLength];
        System.arraycopy(this.data, this.mapOffset, mapCopy, 0, this.mapLength);
        return mapCopy;
    }

    public void print() throws IOException {
        String rowLabel = getRowLabel();
        String columnLabel = getColumnLabel();
        int timestamp = getTimeStamp();
        String value = getValue();
        System.out.println("{RowLabel:" + rowLabel + ", ColumnLabel:" + columnLabel + ", TimeStamp:" + timestamp + ", Value:" + value + "}");
    }

    public short size() {
        return ((short) (this.fieldOffset[fieldCount] - this.mapOffset));
    }

    public void mapInit(byte[] amap, int offset) {
        this.data = amap;
        this.mapOffset = offset;
    }

    public void mapSet(byte [] record, int offset, int length)
    {
        System.arraycopy(record, offset, data, 0, length);
        mapOffset = 0;
        mapLength = length;
    }
    private void setFieldOffsetFromData() throws IOException {
        int position = this.mapOffset + 2;
        this.fieldOffset = new short[NUM_FIELDS + 1];

        for (int i = 0; i <= NUM_FIELDS; i++) {
            this.fieldOffset[i] = Convert.getShortValue(position, this.data);
            position += 2;
        }
    }

    @Override
    public String toString() {
        String rowLabel = null;
        String columnLabel = null;
        String value = null;
        int timestamp = 0;
        try {
            rowLabel = getRowLabel();
            columnLabel = getColumnLabel();
            timestamp = getTimeStamp();
            value = getValue();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String s = new String("{RowLabel:" + rowLabel + ", ColumnLabel:" + columnLabel + ", TimeStamp:" + timestamp + ", Value:" + value + "}");
        return s;
    }
    public static final int MAX_MAP_LENGTH = MINIBASE_PAGESIZE;
    public void setDefaultHdr() throws IOException, InvalidTupleSizeException {
        short numFlds = 4;
        if ((numFlds + 2) * 2 > MAX_MAP_LENGTH)
            throw new InvalidTupleSizeException(null, "MAP: MAP_TOOBIG_ERROR");
        fieldCount = numFlds;
        Convert.setShortValue(numFlds, mapOffset, data);
        fieldOffset = new short[numFlds + 1];
        int pos = mapOffset + 2; // Used first 2 bytes from map_offset tp set numFlds short value in data

        fieldOffset[0] = (short) ((numFlds + 2) * 2 + mapOffset);
        Convert.setShortValue(fieldOffset[0], pos, data);
        pos += 2; //Another 2 bytes used to store the fldOffset[0] which is basically denoting the start of actual data

        // We know that the attribute type orders are String, String, Integer and String.
        short incr = 0;
        for(int i = 1; i<=numFlds; i++){
            if(i == 3){
                incr = 4;
            }else if(i == 1){
                incr = (short) (DEFAULT_STRING_ATTRIBUTE_SIZE + 2);
            }else{
                incr = (short) (DEFAULT_STRING_ATTRIBUTE_SIZE + 2);  //strlen in bytes = strlen +2
            }
            fieldOffset[i] = (short) (fieldOffset[i - 1] + incr);
            Convert.setShortValue(fieldOffset[i], pos, data);
            pos += 2;
        }
        mapLength = fieldOffset[numFlds] - mapOffset;
        if(mapLength > MAX_MAP_LENGTH){
            throw new InvalidTupleSizeException(null, "Map: MAP_TOOBIG_ERROR_AFTER_ALLOC");
        }
    }


    public void setHdr (short numFlds,  AttrType types[], short strSizes[])
            throws IOException, InvalidTypeException, InvalidTupleSizeException
    {
        if((numFlds +2)*2 > MAX_SIZE)
            throw new InvalidTupleSizeException (null, "TUPLE: TUPLE_TOOBIG_ERROR");

        fieldCount = numFlds;
        Convert.setShortValue(numFlds, mapOffset, data);
        fieldOffset = new short[numFlds+1];
        int pos = mapOffset+2;  // start position for fldOffset[]

        //sizeof short =2  +2: array siaze = numFlds +1 (0 - numFilds) and
        //another 1 for fldCnt
        fieldOffset[0] = (short) ((numFlds +2) * 2 + mapOffset);

        Convert.setShortValue(fieldOffset[0], pos, data);
        pos +=2;
        short strCount =0;
        short incr;
        int i;

        for (i=1; i<numFlds; i++)
        {
            switch(types[i-1].attrType) {

                case AttrType.attrInteger:
                    incr = 4;
                    break;

                case AttrType.attrReal:
                    incr =4;
                    break;

                case AttrType.attrString:
                    incr = (short) (strSizes[strCount] +2);  //strlen in bytes = strlen +2
                    strCount++;
                    break;

                default:
                    throw new InvalidTypeException (null, "TUPLE: TUPLE_TYPE_ERROR");
            }
            fieldOffset[i]  = (short) (fieldOffset[i-1] + incr);
            Convert.setShortValue(fieldOffset[i], pos, data);
            pos +=2;

        }
        switch(types[numFlds -1].attrType) {

            case AttrType.attrInteger:
                incr = 4;
                break;

            case AttrType.attrReal:
                incr =4;
                break;

            case AttrType.attrString:
                incr =(short) ( strSizes[strCount] +2);  //strlen in bytes = strlen +2
                break;

            default:
                throw new InvalidTypeException (null, "TUPLE: TUPLE_TYPE_ERROR");
        }

        fieldOffset[numFlds] = (short) (fieldOffset[i-1] + incr);
        Convert.setShortValue(fieldOffset[numFlds], pos, data);

        mapLength = fieldOffset[numFlds] - mapOffset;

        if(mapLength > MAX_SIZE)
            throw new InvalidTupleSizeException (null, "TUPLE: TUPLE_TOOBIG_ERROR");
    }



    public String getGenericValue(String field) throws Exception {
        if (field.matches(".*row.*")) {
            return this.getRowLabel();
        } else if (field.matches(".*column.*")) {
            return this.getColumnLabel();
        } else if (field.matches(".*value.*")) {
            return this.getValue();
        } else {
            throw new Exception("Invalid field type.");
        }
    }

    public Map setStrFld(int fldNo, String val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            Convert.setStrValue(val, fieldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    public Map setIntFld(int fldNo, int val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            Convert.setIntValue(val, fieldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    public Map setFloFld(int fldNo, float val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            Convert.setFloValue(val, fieldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");

    }

    public int getIntFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {
        int val;
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            val = Convert.getIntValue(fieldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    public float getFloFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException {
        float val;
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            val = Convert.getFloValue(fieldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    public String getStrFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException {
        String val;
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            val = Convert.getStrValue(fieldOffset[fldNo - 1], data,
                    fieldOffset[fldNo] - fieldOffset[fldNo - 1]); //strlen+2
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    public AttrType[] getAttrType(){
        AttrType[] mapAttrtypes = new AttrType[4];
        mapAttrtypes[0] = new AttrType (AttrType.attrString);
        mapAttrtypes[1] = new AttrType (AttrType.attrString);
        mapAttrtypes[2] = new AttrType (AttrType.attrInteger);
        mapAttrtypes[3] = new AttrType (AttrType.attrString);

        return mapAttrtypes;
    }
    public short[] getMapSizes(){
        //bigt temp = new bigt();
        short[] mapSizes = new short[3];
        mapSizes[0] = 30;
        mapSizes[1] = 30;
        mapSizes[2] = 30;

        return mapSizes;
    }



}