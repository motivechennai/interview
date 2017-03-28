package com.alcatel.motive.interview;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;



public class FirstHBaseClient {

    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
    	
        byte[] colFamily = Bytes.toBytes("d");
        byte[] qualifier = Bytes.toBytes("c");
    	
    	Table table = new MyConfiguration().getTable(colFamily, qualifier);
 
        //updating the datable with random entries
        addingContacts(table);
        
        while(1>0){
        
        int option = userMenu();
        
        switch(option){
        	
        case 1:
        	System.out.println("Adding a new contact");
        	System.out.println("Enter the Name : ");
            Scanner s1 = new Scanner(System.in);
            byte[] rowKey = Bytes.toBytes(s1.next());
            
        	System.out.println("Enter the PhoneNumber : ");
            Scanner s2 = new Scanner(System.in);
            byte[] value = Bytes.toBytes(s2.nextInt());

            //adding into table
            Put sample = new Put(rowKey);
            sample.add(colFamily, qualifier, value);
            
            table.put(sample);
            System.out.println("Successfully added a new contact");
            System.out.println();

            break;
            
        case 2:
        	System.out.println("Deleting a contact..");
        	System.out.println("Enter the Name : ");
            Scanner s11 = new Scanner(System.in);
            byte[] rowKey1 = Bytes.toBytes(s11.next());
            
            Delete del = new Delete(rowKey1);
            table.delete(del);
            System.out.println("Successfully removed the contact");
            System.out.println();

            break;
            
        case 3:
        	System.out.println("Editing a contact..");
        	System.out.println("Enter the contact name to be edited : ");
        	
            Scanner s12 = new Scanner(System.in);
            byte[] rowKey12 = Bytes.toBytes(s12.next());
            
            System.out.println("Enter the new contact number ..");
            
            Scanner s22 = new Scanner(System.in);
            byte[] value1 = Bytes.toBytes(s22.nextInt());
            
            //adding into table
            Put sample1 = new Put(rowKey12);
            sample1.add(colFamily, qualifier, value1);
            
            table.put(sample1);
            System.out.println("Successfully updated the contact");
            System.out.println();
            break;
            
        case 4:
        	System.out.println("Searching a contact name..");

        	System.out.println("Enter the contact name to be searched : ");
        	
            Scanner ss = new Scanner(System.in);
            byte[] rowKeyy = Bytes.toBytes(ss.next());
            
            Get g = new Get(rowKeyy);
            Result r = table.get(g);
            if(r != null){
            byte[] num = r.getValue(colFamily, qualifier);
            System.out.println("Contact found in the database.");
            System.out.println(Bytes.toString(rowKeyy) + ":" + Bytes.toInt(num));
            }
            else
            	System.out.println("Sorry.. Contact Unavailable");
            System.out.println();

            break;
            
        case 5:
        	System.out.println("Listing all contacts");
        	Scan sc = new Scan();
        	sc.addColumn(colFamily, qualifier);
        	
        	ResultScanner scanner = table.getScanner(sc);
        	
        	for(Result res = scanner.next(); res!=null; res= scanner.next()){
        		System.out.println(Bytes.toString(res.getRow()));
        	}
            System.out.println();
            break;
            
        default:
        	System.out.println("Invalid option..");
        }
        }

    }

	private static int userMenu() {
		System.out.println();
        System.out.println("User Menu : ");
        System.out.println("Add a contact .. Press 1");
        System.out.println("Delete a contact .. Press 2");
        System.out.println("Edit a contact .. Press 3");
        System.out.println("Search by name .. Press 4");
        System.out.println("List all contacts .. Press 5");
        
        Scanner s = new Scanner(System.in);
		return s.nextInt();
		
        
	}

	@SuppressWarnings("deprecation")
	private static void addingContacts(Table table) {
		
		String[] names = {"tarun", "prem", "gopal", "arun", "raj"};
		String[] num = {"1234","5678","3333","2345","1111"};
		for(int i=0;i<names.length;i++){
            byte[] rowKey = Bytes.toBytes(names[i]);
            byte[] val = Bytes.toBytes(num[i]);

            //adding into table
            Put sample = new Put(rowKey);
            sample.add(Bytes.toBytes("d"), Bytes.toBytes("c"), val);
            
            try {
				table.put(sample);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
    /**Get g = new Get(rowKey);
    
    Result r = table.get(g);

    byte[] num = r.getValue(colFamily, qualifier);
    
    System.out.println("The number is : ");
    System.out.println(Bytes.toInt(num));**/
}