package tests;

import java.io.*;

import diskmgr.pcounter;
import global.*;
import heap.*;
import iterator.*;
import bigT.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;

class MainTest implements GlobalConst {

    public static HashSet<String> allBigT;

    public static void display(){
//        SystemDefs.JavabaseDB.pcounter.initialize();
        System.out.println("------------------------ BigTable Tests --------------------------");
        System.out.println("Press 1 for Batch Insert");
        System.out.println("Press 2 for Query");
        System.out.println("Press 3 for MapInsert");
        System.out.println("Press 4 for RowJoin");
        System.out.println("Press 5 for RowSort");
        System.out.println("Press 6 for getCounts");
        System.out.println("Press 7 for other options");
        System.out.println("Press 8 to quit");
        System.out.println("------------------------ BigTable Tests --------------------------");
    }

    public static void displayOtherOptions(){
        System.out.println("----------------------Other Utility functions----------------------");
        System.out.println("Press 1 for Normal Scan");
        System.out.println("Press 2 for Row label count");
        System.out.println("Press 3 for Column label count");
        System.out.println("Press 4 to quit this mode");
    }

    public static void main(String argv[]) throws HFDiskMgrException, HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException, InvalidMapSizeException {
        //original code

        String dbpath = "/tmp/maintest" + System.getProperty("user.name") + ".minibase-db";
        String logpath = "/tmp/maintest" + System.getProperty("user.name") + ".minibase-log";
        SystemDefs sysdef = null;

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        sysdef = new SystemDefs(dbpath, 100000, 1000, "Clock");
        display();
        Scanner sc = new Scanner(System.in);
        String option = sc.nextLine();
        bigT big = null;
        int pages = 0;
        String replacement_policy = "Clock";
        allBigT = new HashSet<>();
        while(!option.equals("8")){              //Code style: It's little annoying using if ,else if , else if.... instead of switch
            //And the each option better map to an class in the test package exactly(instead of in other package), and keep as less as codes in main()

            if(option.equals("1")){             //Issue on BatchInsert: the input buffersize not be used.
                System.out.println("FORMAT: batchinsert DATAFILENAME TYPE BIGTABLENAME");
                String batch = sc.nextLine();
                String[] splits = batch.split(" ");
                if(splits.length!=4){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }
//                dbpath = "/tmp/" + splits[3] + ".minibase-db";
                pcounter.initialize();
                try{
                    long startTime = System.nanoTime();
//                    sysdef.changeNumberOfBuffers(Integer.parseInt(splits[4]), replacement_policy);

                    big = new bigT(splits[3], 1);

                    Batchinsert batchInsert = new Batchinsert(big, splits[1], Integer.parseInt(splits[2]), splits[3]);
                    System.out.println("before insert");
                    //Batchinsert insert  = new Batchinsert();

                    System.out.println("after insert");
                    long endTime = System.nanoTime();
                    allBigT.add(splits[3]);
                    System.out.println("TIME TAKEN "+((endTime - startTime)/1000000000) + " s");
                    System.out.println("Reads : " + pcounter.rcounter);
                    System.out.println("Writes: " + pcounter.wcounter);

                }
                catch(Exception e){
                    System.out.println("Error Occured");
                    e.printStackTrace();
                    display();
                    option = sc.nextLine();
                    continue;
                }


            }else if (option.equals("2")){
                System.out.println("FORMAT: query BIGTABLENAME ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");
                String query = sc.nextLine();
                String[] splits = query.split(" ");
                if(splits.length!=7){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }

            }else if(option.equals("3")){
                System.out.println("FORMAT: mapinsert ROWLABEL COLUMNLABEL VALUE TIMESTAMP TYPE BIGTABLENAME NUMBUF");
                String[] splits = sc.nextLine().split(" ");
                if(splits.length!=8){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }

            }else if(option.equals("4")){
                System.out.println("FORMAT: rowjoin BTNAME1 BTNAME2 OUTBTNAME COLUMNFILTER NUMBUF");
                String[] splits = sc.nextLine().split(" ");
                if(splits.length!=6){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }

                long startTime = System.nanoTime();

                long endTime = System.nanoTime();
                System.out.println("TIME TAKEN "+((endTime - startTime)/1000000000) + " s");

            }else if (option.equals("5")){
                System.out.println("FORMAT: rowsort INBTNAME OUTBTNAME ROWORDER COLUMNNAME NUMBUF");
                System.out.println("ROWORDER: \n 1. Ascending\n 2. Descending" );
                String[] splits = sc.nextLine().split(" ");
                if(splits.length!=6){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }

                long startTime = System.nanoTime();

                long endTime = System.nanoTime();
                System.out.println("TIME TAKEN "+((endTime - startTime)/1000000000) + " s");

            }else if(option.equals("6")){
                System.out.println("FORMAT: getCounts NUMBUF");
                String[] splits = sc.nextLine().split(" ");
                if(splits.length!=2){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }

                long startTime = System.nanoTime();

                long endTime = System.nanoTime();
                System.out.println("TIME TAKEN "+((endTime - startTime)/1000000000) + " s");
            }else if (option.equals("7")){
                System.out.println("Enter BigTable name");
                String bigt_name = sc.nextLine();
                displayOtherOptions();
                String otherOption = sc.nextLine();
                while(!otherOption.equals("4")){
                    if(otherOption.equals("1")){

                        System.out.println("RECORD COUNT: " + big.getMapCnt());
                    }

                    else if(otherOption.equals("2")){

                    }else if(otherOption.equals("3")){

                    }
                    displayOtherOptions();
                    otherOption = sc.nextLine();
                }
            }

            display();
            option = sc.nextLine();
        }

        remove_dbcmd = remove_cmd + dbpath;
        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (Exception e) {
            System.err.println("" + e);
        }

        System.out.println("--------------- BigTable Tests Complete --------------------------");

    }


}