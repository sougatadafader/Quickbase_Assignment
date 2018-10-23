package com.quickbase;

import com.quickbase.devint.ConcreteStatService;
import com.quickbase.devint.DBManager;
import com.quickbase.devint.DBManagerImpl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;

/**
 * The main method of the executable JAR generated from this repository. This is to let you
 * execute something from the command-line or IDE for the purposes of demonstration, but you can choose
 * to demonstrate in a different way (e.g. if you're using a framework)
 */
public class Main {
	
	/* Function Name: getMapfromResultSet
	 * 
	 * Receives a ResultSet
	 * and returns a 
	 * Map in <String,Integer> format
	 * where the String represents a Country and the Integer is the population
	 * */
	
	public static Map<String,Integer> getMapfromResultSet(ResultSet rs) throws SQLException
	{
		Map<String,Integer> dbResult = new HashMap<String,Integer>();
		
		while (rs.next()) {
			dbResult.put(rs.getString(1).trim(), rs.getInt(2));
  	    }
		return dbResult;
	}
	
	
	/* Function Name: getMapFromConList
	 * Receives a List fetched from the concrete class
	 * and returns a 
	 * Map in <String,Integer> format
	 * where the String represents a Country and the Integer is the population
	 * */
	public static Map<String,Integer> getMapFromConList()
	{
		ConcreteStatService con = new ConcreteStatService();
		List<Pair<String, Integer>> conList = new ArrayList<Pair<String, Integer>>();
		conList = con.GetCountryPopulations();
		Map<String,Integer> concreteMap = new HashMap<String,Integer>();
		
		int i =0;
		int length =conList.size();
		String key;
		int val;
		while(i<length)
		{
			key = conList.get(i).getLeft().trim();
			val = conList.get(i).getRight();
			concreteMap.put(key, val);
			i++;
			
		}
		return concreteMap;
	}
	
	/* Function Name: aggregatedList
	 * Receives two Maps, both containing Country and Population entries
	 * and returns a 
	 * Map in <String,Integer> format
	 * which is aggregated. The resultant map contains a key which is a 
	 * String representing a Country and an Integer representing population
	 * */
	public static Map<String,Integer> aggregatedList(Map<String,Integer> dbMap, Map<String,Integer> conMap)
	{
		Map<String,Integer> aggOutput = new HashMap<String,Integer>(dbMap);
		System.out.println("\n\n**Before Aggregation**\n");
		System.out.println("\nData From DataBase\n");
		displayMap(aggOutput);
		System.out.println("\n\nData From Concrete Class\n\n");
		displayMap(conMap);
		
		for (Map.Entry<String, Integer> entry : conMap.entrySet()) {
		    if(!(aggOutput.containsKey(entry.getKey())))
		    {
		    	aggOutput.put(entry.getKey(), entry.getValue());
		    }
		}
		System.out.println("\n\n**After Aggregation**\n");
		displayMap(aggOutput);
		return aggOutput;
	}
	
	
	/* Function Name: displayMap
	 * An utility method to display the values inside a Map.
	 * */
	public static void displayMap(Map<String,Integer> map) {
		System.out.println("===========C O U N T R Y===========================P O P U L A T I O N=======================");
		for (Map.Entry<String,Integer> entry : map.entrySet())  
            System.out.println( entry.getKey() + " \t\t\t\t\t\t "
            		+ entry.getValue());
	}
	
    public static void main( String args[] ) throws SQLException {
        System.out.println("Starting.");
        System.out.print("Getting DB Connection...");

        DBManager dbm = new DBManagerImpl();
        Connection c = dbm.getConnection();
        Map<String,Integer> dbResult = new HashMap<String,Integer>();
        
        if (null == c ) {
            System.out.println("failed.");
            System.exit(1);
        }
        else
        {
        	ResultSet rs = null;
        	PreparedStatement statement = null;
        	String query ="Select c.CountryName, sum(ct.population) As Population\n" + 
        			"from Country as c \n" + 
        			"INNER JOIN State s ON c.CountryId = s.CountryId\n" + 
        			"INNER JOIN City ct ON s.stateId = ct.StateId group by c.CountryId";
        	
        	statement = c.prepareStatement(query);
        	rs = statement.executeQuery();	 
        	dbResult = getMapfromResultSet(rs);
        	
        }
        System.out.println("\n\n\n\n\n");
        Map<String,Integer> concreteMap = getMapFromConList();
        aggregatedList(dbResult,concreteMap);
         
    } 
        

    }