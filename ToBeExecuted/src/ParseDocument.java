
//Takes the input file and parses to find the Document and content associated with the document.Then 
//removes the stopword from the content. 

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.*;
import java.sql.*;

public class ParseDocument {
	//Parses the file and reads each document and content removes stopwords and punctuation marks.
	int sizeOfDataSet=0;
	public HashMap<Integer, String> GetFileData(String FilePath) throws IOException {
		
		HashMap<Integer, String> IDString = new HashMap<Integer, String>();
		BufferedReader br = new BufferedReader(new FileReader(FilePath));
		BufferedReader br1 = new BufferedReader(
				new FileReader("C:\\Users\\ruchi\\ToBeExecuted\\cran\\english.stop.doc"));
		String s1 = "";
		String regex = "(\\b";
		while ((s1 = br1.readLine()) != null) {
			regex = regex + s1 + "\\b|\\b";
		}
		String regex1 = regex.substring(0, regex.length() - 3) + ")";
		String s = "",terms = "";
		int key = 0;
		s = br.readLine();

		while (s != null) {
			sizeOfDataSet++;
			if ((s.charAt(0) == '.' && s.charAt(1) == 'I' && s.charAt(2) == ' ' && Character.isDigit(s.charAt(3)))) {
				key = Integer.parseInt(s.replaceAll("[//.I]", "").trim());
				s = br.readLine();
			} else {
				s = br.readLine();
			}
			if (s.charAt(0) == '.' && s.charAt(1) == 'W') {
				terms = "";
				s = br.readLine();

				while (s.charAt(0) != '.' || s.charAt(1) != 'I' || s.charAt(2) != ' ') {
					//s = s.trim().replaceAll("[-\\.;,()'/-:0-9]", " ");
					s = s.trim().replaceAll("[^a-zA-Z]", " ");
					terms = terms + " " + s;
					s = br.readLine();
					if (s == null)
						break;
				}

				terms = terms.replaceAll(regex1, "");
				terms = terms.trim().replaceAll("\\s+", " ");
				// System.out.println(key+" "+terms);
                if(!terms.isEmpty()) 
				IDString.put(key, terms);                
			}
		}

		br.close();
		return (IDString);

	}
	//convert in inverted index with term and count of that term in each document
	public void InvertedIndex(HashMap<Integer,ArrayList<String>> token)
	{
		int min=100000,max=0,average=0;
		HashMap<String, HashMap<String, Integer>> wordToDocumentMap=new HashMap<String, HashMap<String, Integer>>();
		HashMap<String, Integer> documentToCountMap=new HashMap<String,Integer>();
		ArrayList<String> checkvalue=new ArrayList<String>();
		 for(Integer data:token.keySet())
    		{
    			//System.out.println(data.toString()+" "+token.get(data).toString());
    			ArrayList<String> value=token.get(data);	    			
    			for(int i=0;i<value.size();i++)
    			{
    				if(!wordToDocumentMap.containsKey(value.get(i).toString()))
    				{
    				for(Integer data1:token.keySet())
    				{
    			    checkvalue=token.get(data1);
    			    int count=Collections.frequency(checkvalue, value.get(i));
    			    if(count!=0)
    				documentToCountMap.put(data1.toString(),count );
    			
    				}	    				
    				wordToDocumentMap.put(value.get(i).toString(), documentToCountMap);
    				if(documentToCountMap.size()<min)
    					min=documentToCountMap.size();
    				if(documentToCountMap.size()>max)
    					max=documentToCountMap.size();
    				documentToCountMap.clear();
    				}
    			}   			
    			
    		}
		 System.out.println("Unique words:"+wordToDocumentMap.size());
		 System.out.println("No of postings list:"+wordToDocumentMap.size());
		 System.out.println("Maximum posting list:"+max);
		 System.out.println("Minimum posting list:"+min);
		 average=(min+max)/2;
		 System.out.println("Average length of posting:"+average);
		 System.out.println("Size of inverted index to size of Dataset:"+wordToDocumentMap.size()+"/"+sizeOfDataSet);
		 

		
		
	}
	
	public void QueryTermCount(HashMap<Integer,ArrayList<String>> token)
	{
		Connection c=null;
		Statement stmt=null;
		String sql=null;
		try {
	        Class.forName("org.postgresql.Driver");
	        c = DriverManager
	           .getConnection("jdbc:postgresql://localhost:5432/Irsassignment",
	           "postgres", "ruchita");
	        System.out.println("Opened database successfully");
	        stmt=c.createStatement();
	        for(Integer data:token.keySet())
	    		{
	    			//System.out.println(data.toString()+" "+token.get(data).toString());
	    			ArrayList<String> value=token.get(data);
	    			for(int i=0;i<value.size();i++)
	    			{
	    				try
	    				{
	    				//System.out.println(value.get(i) + ": " + Collections.frequency(value, value.get(i)));
	    				sql="INSERT INTO Query(QUERYID,TERM,TERMFREQUENCY) VALUES ('" +
	    						data.toString()+"','"+value.get(i)+"','"+Collections.frequency(value, value.get(i))+"')";
	    				stmt.executeUpdate(sql);
	    				}
	    				catch(Exception e)
	    				{}
	    			}}
		}
		catch(Exception e)
		{
			System.exit(0);
		}

		
		
	}
	
	public void WeightedFrequency()
	{
		
		Connection c=null;
		Statement stmt=null;
		String id,term;
		int frequency=0,max=1;
		String sql=null;
		ResultSet rs;
		float weight=0;
		try {
	        Class.forName("org.postgresql.Driver");
	        c = DriverManager
	           .getConnection("jdbc:postgresql://localhost:5432/Irsassignment",
	           "postgres", "ruchita");
	        System.out.println("Opened database successfully");
	        stmt=c.createStatement();
	        sql="select * from invertedindex";
	    	rs=stmt.executeQuery(sql);
	    	while(rs.next())
	    	{
	    		id=rs.getString("documentid");
	    		frequency=rs.getInt("count");
	    		term=rs.getString("term");
	    		Statement stmt1=c.createStatement();
	    		sql="select max(count) from (select count from invertedindex where documentid='"+id+"') as dt";
	    		ResultSet rs1=stmt1.executeQuery(sql);
	    		if(rs1.next())
	    		max=rs1.getInt(1);
	    		weight=(float)frequency/max;
	    		sql="UPDATE INVERTEDINDEX SET weightedfreq="+weight+"WHERE term='"+term+"' and documentid='"+id+"'";
	    		stmt1.executeUpdate(sql);
	    	}
	    	
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
		
	}
	
	
	/*public void InvertedIndex(HashMap<Integer,ArrayList<String>> token)
	{
	Connection c=null;
	Statement stmt=null;
	String sql=null;
	int max=0,min=0,avg;
	try {
        Class.forName("org.postgresql.Driver");
        c = DriverManager
           .getConnection("jdbc:postgresql://localhost:5432/Irsassignment",
           "postgres", "ruchita");
        System.out.println("Opened database successfully");

        stmt = c.createStatement();
        for(Integer data:token.keySet())
    		{
    			//System.out.println(data.toString()+" "+token.get(data).toString());
    			ArrayList<String> value=token.get(data);
    			for(int i=0;i<value.size();i++)
    			{
    				try
    				{
    				//System.out.println(value.get(i) + ": " + Collections.frequency(value, value.get(i)));
    				sql="INSERT INTO INVERTEDINDEX(TERM,DOCUMENTID,COUNT,weightedfreq) VALUES ('" +
    					value.get(i)+"',"+data.toString()+",'"+Collections.frequency(value, value.get(i))+"',0.0)";
    				stmt.executeUpdate(sql);
    				}
    				catch(Exception e)
    				{}
    			}}
    	sql="select count(*) from (select term,count(term) from invertedindex group by term) as dt";
    	ResultSet rs=stmt.executeQuery(sql);
    	if(rs.next())
    	System.out.println("Total number of unique words are:"+rs.getString(1));
    	sql="select max(aa) from (select term,count(term)as aa from invertedindex group by term) as dt";
    	ResultSet rs1=stmt.executeQuery(sql);
    	if(rs1.next()) {
    		max=rs1.getInt(1);
    	System.out.println("Maximum no of postings:"+max);}
    	sql="select min(aa) from (select term,count(term)as aa from invertedindex group by term) as dt";
    	ResultSet rs2=stmt.executeQuery(sql);
    	if(rs2.next()) {
    		min=rs2.getInt(1);
    	System.out.println("Minimum no of postings:"+min);}
    	avg=(max+min)/2;
    	System.out.println("Average no of postings:"+avg);
        stmt.close();
        c.close();
     } catch ( Exception e ) {
        System.err.println( e.getClass().getName()+": "+ e.getMessage() );
        System.exit(0);
     }
		
	}
*/	
}
