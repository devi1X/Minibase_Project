package tests;

import bigT.Stream;
import bigT.bigT;
import bigT.Batchinsert;
import diskmgr.pcounter;
import global.GlobalConst;
import global.SystemDefs;
import heap.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class MainTest {
    public static void main(String[] args) throws HFDiskMgrException, InvalidTupleSizeException, InvalidMapSizeException, IOException, InvalidSlotNumberException, HFBufMgrException, HFException {
        bigT bigTable = null;
        int choice = Menu();
        while (choice != 3){
            switch (choice){
                case 1:
                    System.out.println("FORMAT: batchinsert DATAFILENAME TYPE BIGTABLENAME NUMBUF");
                    Scanner sc = new Scanner(System.in);
                    String batch = sc.nextLine();
                    String[] splits = batch.split(" ");
//                    if(splits.length!=5){
//                        System.out.println("Wrong format, try again!");
//                        continue;
//                    }
                    String dataFileName = splits[1];
                    int type = Integer.parseInt(splits[2]);
                    String bigTableName = splits[3];
                    int NUMBUF = Integer.parseInt(splits[4]);
                    //SystemDefs.JavabaseDB.pcounter.initialize();
                    pcounter.initialize();
//                    Batchinsert in = new Batchinsert();
                    Batchinsert bi = new Batchinsert(dataFileName,type,bigTableName,NUMBUF);
                    System.out.println("-------------Start Loading Data---------");

                    //bi.runBatchInsert();
                    bigTable = bi.runInsertTest();
                    //System.out.println(bigTable._firstDirPageId);
                    System.out.println("-------------Finish Loading Data---------");

                    System.out.println("The read count for insert test is: " + pcounter.rcounter);
                    System.out.println("The write count for insert test is: " + pcounter.wcounter);
                    System.out.println("Total num of inserted record: " + bi.insertCount);
                    System.out.println("MapCnt" + bigTable.getMapCnt());
                    break;
                case 2:
                    pcounter.initialize();
                    QueryTest queryTest = new QueryTest();
                    System.out.println("-------------Start Query Test---------");
                    queryTest.runQueryTest(bigTable);
                    System.out.println("-------------Finish Query Test---------");

                    // r/w
                    System.out.println("The read count for query test is: " + pcounter.rcounter);
                    System.out.println("The write count for query test is: " + pcounter.wcounter);
                    break;
                default:
                    System.out.println("Please enter 1, 2 or 3");
            }
            choice = Menu();
        }
    }

    public static int Menu(){
        System.out.println("--------------Start Test-----------");
        System.out.println("[1] Insert Data");
        System.out.println("[2] Query Test");
        System.out.println("[3] Exit");
        System.out.println("-----------------------------------");
        System.out.println("Please enter your choice:");

        return Integer.parseInt(getCommand());
    }

    public static String getCommand(){
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
