package cl.uchile.wikidata.query;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.Op1;
import org.apache.jena.sparql.algebra.op.Op2;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpN;
import org.apache.jena.sparql.algebra.op.OpPath;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.semanticweb.yars.nx.Node;

import cl.uchile.dcc.blabel.label.GraphColouring.HashCollisionException;
import cl.uchile.wikidata.query.ParseBGPsARQ.BGP;

public class ParsePathsARQ {
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
	
	static String[] VARS = {
			"?x", "?y"
	};

	static String[] FILTERQ = {
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
			"rdf-schema#label", 
			"description", 
			"serviceParam",
			"queryHints",
			"qualifier",
			"core#altLabel",
			"rdf-schema#domain",
			"rdf-schema#range",
			"rdf-schema#subclassOf",
			"rdf-schema#subClassOf",
			"rdf-schema#subPropertyOf"
	};
	
	public static String OUTPUT = "paths.tsv";

	public static void main(String[] args) throws UnsupportedEncodingException, IOException, InterruptedException, HashCollisionException {

		int valid = 0;
		int invalid = 0;
		
		int filtered = 0;
		int kept = 0;
		
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT),StandardCharsets.UTF_8));
		TreeSet<String> pathsSeen = new TreeSet<String>();
		TreeSet<String> pathsSigsSeen = new TreeSet<String>();
		
		for(String input:INPUT) {
			BufferedReader br = new BufferedReader(new FileReader(input));

			String line = null;

			while((line=br.readLine())!=null) {
				String[] cols = line.trim().split("\t");
				if(cols.length==4) {
					String queryEString = cols[0];
					String queryString = URLDecoder.decode(queryEString,"UTF-8").replaceAll("\n", " ");

					try {
						Query query = QueryFactory.create(queryString);
						valid++;
						
						Op op = Algebra.compile(query);
						
						ArrayList<OpPath> opPaths = getPaths(op);
						
//						TreeSet<String> opTypes = getOpTypes(op);
						
						for(OpPath opPath: opPaths) {
							String sopt = opPath.toString();
							boolean filter = false;
							if(pathsSeen.add(opPath.toString())) {
								for(String fb:FILTERB) {
									if(sopt.toString().contains(fb)) {
										filter = true;
									}
								}
							} else {
								filter = true;
							}
							
							Query qPath = OpAsQuery.asQuery(opPath) ;
							String qopt = qPath.serialize();
							int si = qopt.indexOf('{');
							int ei = qopt.lastIndexOf('}');
							qopt = qopt.substring(si+1, ei).trim();
							
							String nqopt = qopt+" ";
							ArrayList<String> vars = getVars(qopt);
							for(int i =0; i<vars.size(); i++) {
								nqopt = nqopt.replace(vars.get(i)+" ", VARS[i]+" ");
							}
							nqopt = nqopt.trim();
							
							if(nqopt.contains("!")) {
								filter = true;
							}
							
							if(!pathsSigsSeen.add(nqopt)) {
								filter = true;
							}
							
							if(!filter) {
								pw.println(nqopt);
								//pw.println(qopt+"\t"+queryString+"\t"+cols[2]);
								kept ++;
							} else {
								filtered ++;
							}
						}
					} catch(QueryParseException e) {
						invalid++;
					} catch(ExprEvalException e) {
						invalid++;
					}
					
					System.out.print(".");
					if((valid+invalid)%100==0) {
						System.out.println();
					}
				}
			}
			
			br.close();
		}
		
		pw.close();
		
		System.out.println();
		System.out.println("Parsed queries "+(valid+invalid));
		System.out.println("Valid queries "+valid);
		System.out.println("Invalid queries "+invalid);
		System.out.println("Total Paths "+(kept+filtered));
		System.out.println("Kept Paths "+kept);
		System.out.println("Filtered Paths "+filtered);
	}
	
	private static ArrayList<String> getVars(String qopt) {
		String[] tokens = qopt.split(" ");
		ArrayList<String> vars = new ArrayList<String>();
		for(String token:tokens) {
			if(token.startsWith("?") || token.startsWith("$") || token.startsWith("_")) {
				vars.add(token);
			}
		}
		return vars;
	}

	public static ArrayList<OpPath> getPaths(Op op){
		ArrayList<OpPath> opts = new ArrayList<OpPath>();
		getPaths(op,opts);
		return opts;
	}
	
	public static void getPaths(Op op, ArrayList<OpPath> opPaths){
		if(op instanceof OpPath) {
			opPaths.add((OpPath) op);
		} else if(op instanceof Op1) {
			getPaths(((Op1)op).getSubOp(),opPaths);
		} else if(op instanceof Op2) {
			getPaths(((Op2)op).getLeft(),opPaths);
			getPaths(((Op2)op).getRight(),opPaths);
		} else if(op instanceof OpN) {
			OpN opn = (OpN) op;
			for(Op sop:opn.getElements()) {
				getPaths(sop,opPaths);
			}
		}
	}
	
	public static TreeSet<String> getOpTypes(Op op){
		TreeSet<String> opTypes = new TreeSet<String>();
		getOpTypes(op,opTypes);
		return opTypes;
	}
	
	public static void getOpTypes(Op op, TreeSet<String> opTypes){
		opTypes.add(op.getClass().getSimpleName());
		if(op instanceof Op1) {
			getOpTypes(((Op1)op).getSubOp(),opTypes);
		} else if(op instanceof Op2) {
			getOpTypes(((Op2)op).getLeft(),opTypes);
			getOpTypes(((Op2)op).getRight(),opTypes);
		} else if(op instanceof OpN) {
			OpN opn = (OpN) op;
			for(Op sop:opn.getElements()) {
				getOpTypes(sop,opTypes);
			}
		}
	}
	
	public static class Opt {
		OpLeftJoin opt;

		Opt oleft = null;
		Opt oright = null;
		
		BGP bleft = null;
		BGP bright = null;
		
		public Opt(OpLeftJoin opt) throws InterruptedException, HashCollisionException {
			this.opt = opt;
			if(opt.getLeft() instanceof OpBGP) {
				bleft = new BGP((OpBGP) opt.getLeft());
			} else if(opt.getLeft() instanceof OpLeftJoin) {
				oleft = new Opt((OpLeftJoin) opt.getLeft());
			}
			
			if(opt.getRight() instanceof OpBGP) {
				bright = new BGP((OpBGP) opt.getRight());
			} else if(opt.getRight() instanceof OpLeftJoin) {
				oright = new Opt((OpLeftJoin) opt.getRight());
			}
		}
		
		public TreeSet<Node> getVars() {
			TreeSet<Node> vars = new TreeSet<Node>();
			if(bleft!=null) {
				vars.addAll(bleft.getVars());
			} else if(oleft!=null) {
				vars.addAll(oleft.getVars());
			}
			
			if(bright!=null) {
				vars.addAll(bright.getVars());
			} else if(oright!=null) {
				vars.addAll(oright.getVars());
			}
			
			return vars;
		}
		
		public boolean hasCartesianProducts() {
			TreeSet<Node> lvars = null;
			TreeSet<Node> rvars = null;
			
			if(bleft!=null) {
				if(bleft.clr.getPartitionCount()>1)
					return true;
				lvars = bleft.getVars();
			} else if(oleft!=null) {
				if(oleft.hasCartesianProducts())
					return true;
				lvars = oleft.getVars();
			}
			
			if(bright!=null) {
				if(bright.clr.getPartitionCount()>1)
					return true;
				rvars = bright.getVars();
			} else if(oright!=null) {
				if(oright.hasCartesianProducts())
					return true;
				rvars = oright.getVars();
			}
			
			boolean cartesian = true;
			for(Node left:lvars) {
				if(rvars.contains(left)) {
					cartesian = false;
				}
			}
			
			return cartesian;
		}
		
		public String getSignature() {
			String hash = "";
			
			if(bleft!=null) {
				hash+=bleft.getSignature();
			} else if(oleft!=null) {
				hash+=oleft.getSignature();
			}
			
			hash += "OPT";
			
			if(bright!=null) {
				hash+=bright.getSignature();
			} else if(oright!=null) {
				hash+=oright.getSignature();
			}
			
			return hash;
		}
		
		public String getSignatureVarId() {
			String hash = "";
			
			if(bleft!=null) {
				hash+=bleft.getSignatureVarId();
			} else if(oleft!=null) {
				hash+=oleft.getSignatureVarId();
			}
			
			hash += "OPT";
			
			if(bright!=null) {
				hash+=bright.getSignatureVarId();
			} else if(oright!=null) {
				hash+=oright.getSignatureVarId();
			}
			
			return hash;
		}
		
		public String toString() {
			String left = null;
			String right = null;
			if(bleft!=null) {
				left = bleft.toNonCanonicalisedString();
			} else if(oleft!=null) {
				left = oleft.toString();
			}
			
			if(bright!=null) {
				right = bright.toNonCanonicalisedString();
			} else if(oright!=null) {
				right = oright.toString();
			}
			
			return "{ "+left+" } OPTIONAL { "+right+" }";
		}
		
	}
	
}
