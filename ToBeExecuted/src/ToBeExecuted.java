import java.io.*;
import java.security.KeyStore.Entry;
import java.util.*;
import java.sql.*;

public class ToBeExecuted {
	// converts the string into single word and stores in key list pair performs porters algorithm on each word .
	public static HashMap<Integer, ArrayList<String>> Tokenize(HashMap<Integer, String> KeyDocument) {
		int wordCount = 0;
		ArrayList<String> Data;
		HashMap<Integer, ArrayList<String>> token = new HashMap<Integer, ArrayList<String>>();		
		for (Integer key : KeyDocument.keySet()) {
			String[] words = KeyDocument.get(key).split("\\W+");
			 Data= new ArrayList<String>();
			Stemmer s2 = new Stemmer();
			for (int j=0;j<words.length;j++) {
				for (int i = 0; i < words[j].length(); i++) {
					s2.add(words[j].charAt(i));
				}
				s2.stem();
				String ab = s2.toString();
				Data.add(ab);
			}
			token.put(key, Data);
			wordCount += words.length;
		}
		
		//System.out.println("Total words in the file are:"+ wordCount);
		return (token);
	}

	// Main code to be executed.
	public static void main(String[] args) throws IOException {
		try {
			long startTime = System.nanoTime();
			Scanner sc=new Scanner(System.in);
			String FilePath = "C:\\Users\\ruchi\\ToBeExecuted\\cran\\cran.all.1400";
			String QueryPath="C:\\Users\\ruchi\\ToBeExecuted\\cran\\cran.qry";
			ParseDocument p = new ParseDocument();
			//HashMap<Integer, String> KeyDocument = p.GetFileData(FilePath);
			HashMap<Integer, String> QueryKeyDocument = p.GetFileData(QueryPath);			
			//System.out.println("No of Documents in the File are:" + KeyDocument.size());
			HashMap<Integer, ArrayList<String>> token = Tokenize(QueryKeyDocument);
			//p.WeightedFrequency();
			//p.QueryTermCount(token);
			//p.InvertedIndex(token);
			System.out.println("Enter the document id you want to search:");
			int queryid=sc.nextInt();
			p.FindDocument(queryid);
			long endTime = System.nanoTime();
			long totaltime=(endTime-startTime);
			System.out.println("Total time taken:"+ totaltime);
			System.out.println("Memory in KB:"+(double)((Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())/1024));			
		} catch (Throwable ex) {
			System.err.println("Uncaught exception - " + ex.getMessage());
		}
	}
}
