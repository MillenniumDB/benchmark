package cl.uchile.wikidata.query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.TreeSet;

public class ParseQueriesString {
	static String INPUT_FOLDER = "data/queries/";
	
	static String[] INPUT = {
			INPUT_FOLDER+"I1_status500_Joined.tsv",
			INPUT_FOLDER+"I2_status500_Joined.tsv",
			INPUT_FOLDER+"I3_status500_Joined.tsv",
			INPUT_FOLDER+"I4_status500_Joined.tsv",
			INPUT_FOLDER+"I5_status500_Joined.tsv",
			INPUT_FOLDER+"I6_status500_Joined.tsv",
			INPUT_FOLDER+"I7_status500_Joined.tsv"
	};

	static String[] FILTER = {
			"OPTIONAL",
			"COUNT",
			"UNION",
			"MINUS",
			"FILTER",
			"GROUP BY",
			"ORDER BY",
			"*",
			"+",
			"^",
			"DESCRIBE",
			"CONSTRUCT",
			"VALUES",
			"SERVICE",
			"REGEX",
			"ASK",
			"string",
			"> / <",
			"|",
			"search",
			"OFFSET",
			"CONCAT",
			"SUM",
			" SELECT",
			" AS ",
			"qualifier"
	};

	public static void main(String[] args) throws UnsupportedEncodingException, IOException {

		TreeSet<String> sorted = new TreeSet<String>();
		
		for(String input:INPUT) {
			BufferedReader br = new BufferedReader(new FileReader(input));

			String line = null;

			while((line=br.readLine())!=null) {
				String[] cols = line.trim().split("\t");
				if(cols.length==4 && cols[2].equals("organic")) {
					String equery = cols[0];
					String query = URLDecoder.decode(equery,"UTF-8").replaceAll("\n", " ");

					int srv = query.indexOf("SERVICE");
					if(srv!=-1) {
						int end = query.indexOf("}", srv);
						query = query.substring(0, srv-1) + query.substring(end+1);
					}
					
					
					boolean filter = false;
					for(String f:FILTER) {
						if(query.toLowerCase().contains(f.toLowerCase()))
							filter = true;
					}

					if(!filter) {
						sorted.add(query);
					}
				}
			}
			
			br.close();
		}

		for(String query:sorted) {
			System.out.println(query);
		}


	}
}
