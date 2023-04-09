package tests;

import bigT.Stream;
import bigT.bigT;
import bigT.Batchinsert;
import diskmgr.pcounter;
import global.GlobalConst;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class MainTest {
    public static void main(String[] args){
        bigT bigTable = null;
        int choice = Menu();
        while (choice != 3){
            switch (choice){
                case 1:
                    pcounter.initialize();
                    Batchinsert in = new Batchinsert();
                    System.out.println("-------------Start Loading Data---------");
                    bigTable = in.runInsertTest();
                    System.out.println("-------------Finish Loading Data---------");

                    System.out.println("The read count for insert test is: " + pcounter.rcounter);
                    System.out.println("The write count for insert test is: " + pcounter.wcounter);
                    break;
                case 2:
                    //pcounter.initialize();
                    QueryTest queryTest = new QueryTest();
                    System.out.println("-------------Start Query Test---------");
                    queryTest.runQueryTest(bigTable);
                    System.out.println("-------------Finish Query Test---------");

                    // r/w
//                    System.out.println("The read count for query test is: " + pcounter.rcounter);
//                    System.out.println("The write count for query test is: " + pcounter.wcounter);
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
