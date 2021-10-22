package cl.uchile.wikidata.query;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.Op1;
import org.apache.jena.sparql.algebra.op.Op2;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpN;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.expr.aggregate.AggAvg;
import org.apache.jena.sparql.expr.aggregate.AggAvgDistinct;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountDistinct;
import org.apache.jena.sparql.expr.aggregate.AggCountVar;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;
import org.apache.jena.sparql.expr.aggregate.AggGroupConcat;
import org.apache.jena.sparql.expr.aggregate.AggSum;
import org.apache.jena.sparql.expr.aggregate.AggSumDistinct;
import org.apache.jena.sparql.expr.aggregate.Aggregator;

import cl.uchile.dcc.blabel.label.GraphColouring.HashCollisionException;

public class ParseNumericQueriesARQ {
	static String INPUT_FOLDER = "data/queries/";
	
	static String[] INPUT = {
			INPUT_FOLDER+"2017-06-12_2017-07-09_organic.tsv.gz",
			INPUT_FOLDER+"2017-07-10_2017-08-06_organic.tsv.gz",
			INPUT_FOLDER+"2017-08-07_2017-09-03_organic.tsv.gz",
			INPUT_FOLDER+"2017-12-03_2017-12-30_organic.tsv.gz",
			INPUT_FOLDER+"2018-01-01_2018-01-28_organic.tsv.gz",
			INPUT_FOLDER+"2018-01-29_2018-02-25_organic.tsv.gz",
			INPUT_FOLDER+"2018-02-26_2018-03-25_organic.tsv.gz"
	};
	
	static String PROPERTIES = "data/num-props.txt";

	static String[] FILTERQ = {
//			"OPTIONAL",
//			"COUNT",
//			"UNION",
//			"MINUS",
//			"FILTER",
//			"GROUP BY",
//			"ORDER BY",
//			"*",
//			"+",
//			"^",
//			"DESCRIBE",
//			"CONSTRUCT",
//			"VALUES",
//			"SERVICE",
//			"REGEX",
//			"ASK",
//			"string",
//			"> / <",
//			"|",
//			"search",
//			"OFFSET",
//			"CONCAT",
//			"SUM",
//			" SELECT",
//			" AS ",
//			"qualifier"
	};
	
	static String[] FILTERB = {
			"statement", 
//			"rdf-schema#label", 
//			"description", 
//			"serviceParam",
//			"queryHints",
			"qualifier",
//			"core#altLabel",
//			"rdf-schema#domain",
//			"rdf-schema#range",
//			"rdf-schema#subclassOf",
//			"rdf-schema#subClassOf",
//			"rdf-schema#subPropertyOf"
	};
	
	public static String OUTPUT = "num-queries-v2.tsv";

	public static void main(String[] args) throws UnsupportedEncodingException, IOException, InterruptedException, HashCollisionException {

		int valid = 0;
		int invalid = 0;
		
		int filtered = 0;
		int kept = 0;
		
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT),StandardCharsets.UTF_8));
		TreeSet<String> queriesSeen = new TreeSet<String>();
		
		BufferedReader brp = new BufferedReader(new FileReader(PROPERTIES));
		TreeSet<String> properties = new TreeSet<String>();
		String linep = null;
		
		while((linep=brp.readLine())!=null) {
			linep = linep.trim();
			if(!linep.isEmpty()) {
				properties.add(linep);
			}
		}
		brp.close();
		
		for(String input:INPUT) {
			System.out.print("\nProcessing "+input+"\n");
			InputStream is = new FileInputStream(input);
			if(input.endsWith(".gz")) {
				is = new GZIPInputStream(is);
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(is));

			String line = null;

			while((line=br.readLine())!=null) {
				String[] cols = line.trim().split("\t");
				if(cols.length==4) {
					String queryEString = cols[0];
					String queryString = URLDecoder.decode(queryEString,"UTF-8").replaceAll("\n", " ");

					if(queriesSeen.add(queryString)) {
						try {
							Query query = QueryFactory.create(queryString);
							valid++;
							
							List<Var> vars = query.getProjectVars();
							TreeSet<String> pvars = new TreeSet<String>();
							
							for(Var v:vars) {
								pvars.add(v.toString());
							}
							
							Op op = Algebra.compile(query);
							
							TreeSet<String> numVars = getNumVars(op,properties);
							
							TreeSet<String> pNumVars = new TreeSet<String>();
							
							for(String numVar:numVars) {
								if(pvars.contains(numVar)) {
									pNumVars.add(numVar);
								}
							}
							
							if(!pNumVars.isEmpty() && pvars.size()>pNumVars.size()) {
								String newQueryString = queryString;
								if(queryString.toLowerCase().contains("service")) {
//									System.out.println("\nb4 "+op);
									try {
										Op opNoService = Transformer.transform(new RemoveServiceClauses(), op);
										
										// need to get the top level op
										opNoService = RemoveServiceClauses.transformTopLevel(opNoService);
										
										
	//									System.out.println("\naf "+opNoService);
										query = OpAsQuery.asQuery(opNoService);
										newQueryString = query.toString().replaceAll("\n", " ").replaceAll("  ", " ");
										
	//									if(newQueryString.contains("Label")) {
	//										System.out.println("\n"+newQueryString);
	//										System.out.println(opNoService);
	//									}
									} catch(Exception e) {
										System.err.println(queryString);
										throw e;
									}
								}
								
								pw.println(pNumVars+"\t"+newQueryString+"\t"+queryString);
								kept ++;
							}
							
						} catch(QueryParseException e) {
							invalid++;
						} catch(ExprEvalException e) {
							invalid++;
						}
						
						
						if((valid+invalid)%100==0) {
							System.out.print(".");
							if((valid+invalid)%5000==0) {
								System.out.println();
							}
						}
					} else {
						filtered ++;
					}
				}
			}
			
			br.close();
			
			System.out.println(" done.");
		}
		
		pw.close();
		
		System.out.println();
		System.out.println("Parsed queries "+(valid+invalid));
		System.out.println("Valid queries "+valid);
		System.out.println("Invalid queries "+invalid);
		System.out.println("Total queries "+(kept+filtered));
		System.out.println("Kept queries "+kept);
		System.out.println("Filtered queries "+filtered);
	}
	
	private static TreeSet<String> getNumVars(Op op, TreeSet<String> properties) {
		TreeSet<String> numVars = new TreeSet<String>();
		getNumVars(op,properties,numVars);
		return numVars;
	}

	private static void getNumVars(Op op, TreeSet<String> properties, TreeSet<String> numVars) {
		if(op instanceof OpBGP) {
			OpBGP bgp = (OpBGP)op;
			List<Triple> tps = bgp.getPattern().getList();
			for(Triple tp:tps) {
//				System.out.println(tp.getPredicate().toString()+"\t"+properties.contains(tp.getPredicate().toString())+"\t"+tp.getObject().isVariable());
				if(properties.contains(tp.getPredicate().toString()) && tp.getObject().isVariable()) {
					numVars.add(tp.getObject().toString());
				}
			}
		} else if (op instanceof OpExtend) {
			OpExtend ope = (OpExtend) op;
			
			HashMap<Var,Var> varMap = getVarMap(ope);
			
			Op succ = getNonExtendSuccessor(ope);
			
			if(succ instanceof OpGroup) {
//				System.out.println("ope:"+ope+" exp:"+ope.getVarExprList());
//				System.out.println("mv "+mapVars);
				
				OpGroup opg = (OpGroup) succ;
				for(ExprAggregator agg: opg.getAggregators()) {
//					System.out.println(agg+"\t"+agg.getAggregator().getClass().getName());
					Aggregator ab = agg.getAggregator();
					
					boolean isNumArg = false;
					try{
//						System.out.println("avar "+agg.getExpr().asVar());
						Var v = agg.getExpr().asVar();
						isNumArg = numVars.contains(v.toString());
						
//						System.out.println("nva "+v);
					} catch (Exception e) {
						// not a variable
					}
					if(ab instanceof AggCount ||
							ab instanceof AggCountVar || 
							ab instanceof AggCountDistinct || 
							ab instanceof AggCountVarDistinct || 
							ab instanceof AggAvg || 
							ab instanceof AggAvgDistinct || 
							ab instanceof AggSum || 
							ab instanceof AggSumDistinct || 
							(!(ab instanceof AggGroupConcat) && isNumArg)) {
						try {
							Var mapped = varMap.get(agg.getAggVar().asVar());
							if(mapped != null) {
								numVars.add(mapped.toString());
//								System.out.println("Adding "+mapped+" from\n"+ope);
							} else {
//								System.err.println("Did not find agg variable "+agg.getAggVar().asVar()+" for\n "+ope);
							}
						}
						catch (Exception e) {
							System.err.println(agg.getAggVar()+" cannot be returned as a variable");
						}
					}
				}
			}
	    } else if(op instanceof Op1) {
			getNumVars(((Op1)op).getSubOp(),properties,numVars);
		} else if(op instanceof Op2) {
			getNumVars(((Op2)op).getLeft(),properties,numVars);
			getNumVars(((Op2)op).getRight(),properties,numVars);
		} else if(op instanceof OpN) {
			OpN opn = (OpN) op;
			for(Op sop:opn.getElements()) {
				getNumVars(sop,properties,numVars);
			}
		}
	}
	
	private static HashMap<Var,Var> getVarMap(OpExtend op) {
		HashMap<Var,Var> varMap = new HashMap<Var,Var>();
		getVarMap(op,varMap);
		return varMap;
	}
	
	private static HashMap<Var,Var>  getVarMap(OpExtend op, HashMap<Var,Var> varMap) {
		for(Map.Entry<Var, Expr> e : op.getVarExprList().getExprs().entrySet()) {
			varMap.put(e.getValue().asVar(), e.getKey());
		}
		if(op.getSubOp() instanceof OpExtend) {
			getVarMap((OpExtend)op.getSubOp(),varMap);
		}
		return varMap;
	}
	
	private static Op getNonExtendSuccessor(OpExtend op) {
		if(op.getSubOp() instanceof OpExtend) {
			getNonExtendSuccessor((OpExtend)op.getSubOp());
		}
		return op.getSubOp();
	}
}
