package iterator;


import bigT.Map;
import heap.*;
import global.*;
import java.io.*;
import java.lang.*;


public class MapUtils
{
    
    public static int CompareMapWithMap(int ORDERTYPE, Map t1, int t1_fld_no,
                                        Map t2, int t2_fld_no)
            throws IOException,
            UnknowAttrType,
            MapUtilsException
    {
        int resCompare = -2;

        if (ORDERTYPE == 1) // Compare row label, column label, timestamp
    {
        resCompare = rowCompare(t1, t2);
        if (resCompare == 0)
        {
            resCompare = columnCompare(t1, t2);
            if (resCompare == 0)
            {
                resCompare = compareTimeStamp(t1, t2);
            }
        }
    }
    else if (ORDERTYPE == 2) // Compare column label, row label, timestamp
    {
        resCompare = columnCompare(t1, t2);
        if (resCompare == 0)
        {
            resCompare = rowCompare(t1, t2);
            if (resCompare == 0)
            {
                resCompare = compareTimeStamp(t1, t2);
            }
        }
    }
    else if (ORDERTYPE == 3) // Compare row label and timestamp
    {
        resCompare = rowCompare(t1, t2);
        if (resCompare == 0)
        {
            resCompare = compareTimeStamp(t1, t2);
        }
    }
    else if (ORDERTYPE == 4) // Compare column label and timestamp
    {
        resCompare = columnCompare(t1, t2);
        if (resCompare == 0)
        {
            resCompare = compareTimeStamp(t1, t2);
        }
    }
    else if (ORDERTYPE == 5) // Only compare timestamp
    {
        resCompare = compareTimeStamp(t1, t2);
    }
    else // default
    {
        return resCompare;
    }

    return resCompare;
}
    private static int rowCompare(Map t1, Map t2)
    {
        String t1_f = "";
        String t2_f = "";

        try 
        { 
            t1_f = t1.getRowLabel(); // Get row label of t1
            t2_f = t2.getRowLabel(); // Get row label of t2
        }
        catch(Exception e)
        {
            System.out.println("Exception: " + e);
            return -2;
        }

        // Compare row labels and return result
        int result = t1_f.compareTo(t2_f);

        if (result < 0) {
            return -1;
        } else if (result > 0) {
            return 1;
        } else {
            return 0;
        }
    }


    private static int columnCompare(Map t1, Map t2)
    {
        String t1_f = "";
        String t2_f = "";

        try 
        { 
            t1_f = t1.getColumnLabel(); // Get column label of t1
            t2_f = t2.getColumnLabel(); // Get column label of t2
        }
        catch(Exception e)
        {
            System.out.println("Exception: " + e);
            return -2;
        }

        // Compare column labels and return result
        int result = t1_f.compareTo(t2_f);

        if (result < 0) {
            return -1;
        } else if (result > 0) {
            return 1;
        } else {
            return 0;
        }
    }

    private static int compareTimeStamp(Map t1, Map t2)
    {
        int t1_f = 0;
        int t2_f = 0;

        try 
        { 
            t1_f = t1.getTimeStamp(); // Get timestamp of t1
            t2_f = t2.getTimeStamp(); // Get timestamp of t2
        }
        catch(Exception e)
        {
            System.out.println("Exception: " + e);
            return -2;
        }

        // Compare timestamps and return result
        int result = t1_f - t2_f;

        if (result < 0) {
            return -1;
        } else if (result > 0) {
            return 1;
        } else {
            return 0;
        }
    }


    /**
     * This function compares a map with another map in respective field, and
     *  returns:
     *
     *    0        if the two are equal,
     *    1        if the map is greater,
     *   -1        if the tuple is smaller,
     *
     *@param    fldType   the type of the field being compared.
     *@param    t1        one map.
     *@param    t2        another map.
     *@param    t1_fld_no the field numbers in the tuples to be compared.
     *@param    t2_fld_no the field numbers in the tuples to be compared.
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception TupleUtilsException exception from this class
     *@return   0        if the two are equal,
     *          1        if the tuple is greater,
     *         -1        if the tuple is smaller,
     */
    public static int CompareMapWithMap(AttrType fldType,
                                        Map t1, int t1_fld_no,
                                        Map  t2, int t2_fld_no)
            throws IOException,
            UnknowAttrType,
            MapUtilsException
    {
        int   t1_i,  t2_i;
        float t1_r,  t2_r;
        String t1_s, t2_s;

        switch (fldType.attrType)
        {
            case AttrType.attrInteger:                // Compare two integers.
                try {
                    t1_i = t1.getIntFld(t1_fld_no);
                    t2_i = t2.getIntFld(t2_fld_no);
                }catch (FieldNumberOutOfBoundException e){
                    throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }
                if (t1_i == t2_i) return  0;
                if (t1_i <  t2_i) return -1;
                if (t1_i >  t2_i) return  1;

            case AttrType.attrReal:                // Compare two floats
                try {
                    t1_r = t1.getFloFld(t1_fld_no);
                    t2_r = t2.getFloFld(t2_fld_no);
                }catch (FieldNumberOutOfBoundException e){
                    throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }
                if (t1_r == t2_r) return  0;
                if (t1_r <  t2_r) return -1;
                if (t1_r >  t2_r) return  1;

            case AttrType.attrString:                // Compare two strings
                try {
                    t1_s = t1.getStrFld(t1_fld_no);
                    t2_s = t2.getStrFld(t2_fld_no);
                }catch (FieldNumberOutOfBoundException e){
                    throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }

                // Now handle the special case that is posed by the max_values for strings...
                if(t1_s.compareTo( t2_s)>0)return 1;
                if (t1_s.compareTo( t2_s)<0)return -1;
                return 0;
            default:

                throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

        }
    }



    /**
     * This function  compares  tuple1 with another tuple2 whose
     * field number is same as the tuple1
     *
     *@param    fldType   the type of the field being compared.
     *@param    t1        one tuple
     *@param    value     another tuple.
     *@param    t1_fld_no the field numbers in the tuples to be compared.
     *@return   0        if the two are equal,
     *          1        if the tuple is greater,
     *         -1        if the tuple is smaller,
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception TupleUtilsException exception from this class
     */
    public static int CompareMapWithValue(AttrType fldType,
                                            Map  t1, int t1_fld_no,
                                            Map  value)
            throws IOException,
            UnknowAttrType,
            TupleUtilsException, MapUtilsException {
        return CompareMapWithMap(fldType, t1, t1_fld_no, value, t1_fld_no);
    }

    /**
     *This function Compares two Tuple inn all fields
     * @param t1 the first tuple
     * @param t2 the secocnd tuple
     * @param //type[] the field types
     * @param len the field numbers
     * @return  0        if the two are not equal,
     *          1        if the two are equal,
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception TupleUtilsException exception from this class
     */

    public static boolean Equal(Map t1, Map t2, AttrType types[], int len)
            throws IOException, UnknowAttrType, MapUtilsException, TupleUtilsException {
        int i;

        for (i = 1; i <= len; i++)
            if (CompareMapWithMap(types[i-1], t1, i, t2, i) != 0)
                return false;
        return true;
    }

    /**
     *get the string specified by the field number
     *@param map the tuple
     *@param //fidno the field number
     *@return the content of the field number
     *@exception IOException some I/O fault
     *@exception TupleUtilsException exception from this class
     */
    public static String Value(Map  map, int fldno)
            throws IOException,
            MapUtilsException
    {
        String temp;
        try{
            temp = map.getStrFld(fldno);
        }catch (FieldNumberOutOfBoundException e){
            throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        return temp;
    }


    /**
     *set up a tuple in specified field from a tuple
     *@param value the tuple to be set
     *@param map the given tuple
     *@param fld_no the field number
     *@param fldType the tuple attr type
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception TupleUtilsException exception from this class
     */
    public static void SetValue(Map value, Map  map, int fld_no, AttrType fldType)
            throws IOException,
            UnknowAttrType,
            TupleUtilsException
    {

        switch (fldType.attrType)
        {
            case AttrType.attrInteger:
                try {
                    value.setIntFld(fld_no, map.getIntFld(fld_no));
                }catch (FieldNumberOutOfBoundException e){
                    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }
                break;
            case AttrType.attrReal:
                try {
                    value.setFloFld(fld_no, map.getFloFld(fld_no));
                }catch (FieldNumberOutOfBoundException e){
                    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }
                break;
            case AttrType.attrString:
                try {
                    value.setStrFld(fld_no, map.getStrFld(fld_no));
                }catch (FieldNumberOutOfBoundException e){
                    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }
                break;
            default:
                throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

        }

        return;
    }


    /**
     *set up the Jtuple's attrtype, string size,field number for using join
     *@param Jmap  reference to an actual tuple  - no memory has been malloced
     *@param res_attrs  attributes type of result tuple
     *@param in1  array of the attributes of the tuple (ok)
     *@param len_in1  num of attributes of in1
     *@param in2  array of the attributes of the tuple (ok)
     *@param len_in2  num of attributes of in2
     *@param t1_str_sizes shows the length of the string fields in S
     *@param t2_str_sizes shows the length of the string fields in R
     *@param proj_list shows what input fields go where in the output tuple
     *@param nOutFlds number of outer relation fileds
     *@exception IOException some I/O fault
     *@exception TupleUtilsException exception from this class
     */
    public static short[] setup_op_map(Map Jmap, AttrType[] res_attrs,
                                         AttrType in1[], int len_in1, AttrType in2[],
                                         int len_in2, short t1_str_sizes[],
                                         short t2_str_sizes[],
                                         FldSpec proj_list[], int nOutFlds)
            throws IOException,
            MapUtilsException
    {
        short [] sizesT1 = new short [len_in1];
        short [] sizesT2 = new short [len_in2];
        int i, count = 0;

        for (i = 0; i < len_in1; i++)
            if (in1[i].attrType == AttrType.attrString)
                sizesT1[i] = t1_str_sizes[count++];

        for (count = 0, i = 0; i < len_in2; i++)
            if (in2[i].attrType == AttrType.attrString)
                sizesT2[i] = t2_str_sizes[count++];

        int n_strs = 0;
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer)
                res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
            else if (proj_list[i].relation.key == RelSpec.innerRel)
                res_attrs[i] = new AttrType(in2[proj_list[i].offset-1].attrType);
        }

        // Now construct the res_str_sizes array.
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
            else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
        }

        short[] res_str_sizes = new short [n_strs];
        count         = 0;
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
            else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT2[proj_list[i].offset-1];
        }
        try {
            Jmap.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
        }catch (Exception e){
            throw new MapUtilsException(e,"setHdr() failed");
        }
        return res_str_sizes;
    }


    /**
     *set up the Jtuple's attrtype, string size,field number for using project
     *@param Jmap  reference to an actual tuple  - no memory has been malloced
     *@param res_attrs  attributes type of result tuple
     *@param in1  array of the attributes of the tuple (ok)
     *@param len_in1  num of attributes of in1
     *@param t1_str_sizes shows the length of the string fields in S
     *@param proj_list shows what input fields go where in the output tuple
     *@param nOutFlds number of outer relation fileds
     *@exception IOException some I/O fault
     *@exception TupleUtilsException exception from this class
     *@exception InvalidRelation invalid relation
     */

    public static short[] setup_op_map(Map Jmap, AttrType res_attrs[],
                                         AttrType in1[], int len_in1,
                                         short t1_str_sizes[],
                                         FldSpec proj_list[], int nOutFlds)
            throws IOException,
            MapUtilsException,
            InvalidRelation
    {
        short [] sizesT1 = new short [len_in1];
        int i, count = 0;

        for (i = 0; i < len_in1; i++)
            if (in1[i].attrType == AttrType.attrString)
                sizesT1[i] = t1_str_sizes[count++];

        int n_strs = 0;
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer)
                res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);

            else throw new InvalidRelation("Invalid relation -innerRel");
        }

        // Now construct the res_str_sizes array.
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer
                    && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
        }

        short[] res_str_sizes = new short [n_strs];
        count         = 0;
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key ==RelSpec.outer
                    && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
        }

        try {
            Jmap.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
        }catch (Exception e){
            throw new MapUtilsException(e,"setHdr() failed");
        }
        return res_str_sizes;
    }
}




