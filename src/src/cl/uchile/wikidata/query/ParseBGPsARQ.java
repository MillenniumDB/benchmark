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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.Op1;
import org.apache.jena.sparql.algebra.op.Op2;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpN;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;

import com.google.common.hash.HashCode;

import cl.uchile.dcc.blabel.label.GraphColouring.HashCollisionException;
import cl.uchile.dcc.blabel.label.GraphLabelling;
import cl.uchile.dcc.blabel.label.GraphLabelling.GraphLabellingResult;
import cl.uchile.dcc.blabel.label.util.HashGraph;
import nl.uu.cs.treewidth.algorithm.TreewidthDP;
import nl.uu.cs.treewidth.input.GraphInput.InputData;
import nl.uu.cs.treewidth.ngraph.ListGraph;
import nl.uu.cs.treewidth.ngraph.ListVertex;
import nl.uu.cs.treewidth.ngraph.NGraph;
import nl.uu.cs.treewidth.ngraph.NVertex;

public class ParseBGPsARQ {
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

	static String[] FILTERQ = {
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
	
	public static String OUTPUT = "bgps-normalized.tsv";

	public static void main(String[] args) throws UnsupportedEncodingException, IOException, InterruptedException, HashCollisionException {

		int valid = 0;
		int invalid = 0;
		
		int bgpsfiltered = 0;
		int bgpskept = 0;
		
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT),StandardCharsets.UTF_8));
		TreeSet<String> bgpsSeen = new TreeSet<String>();
		
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
						
						ArrayList<OpBGP> opbgps = getBgps(op);
						
						ArrayList<BGP> bgps = new ArrayList<BGP>();
											
						TreeSet<String> opTypes = getOpTypes(op);
						
						for(OpBGP opbgp:opbgps) {
							BGP bgp = new BGP(opbgp);
							
							// first filter service BGPs
							if(bgp.vars.size()>0) {
								bgps.add(bgp);
							}
						}
						
						for(BGP bgp: bgps) {
							String sbgp = bgp.toString();
							boolean filter = false;
							for(String fb:FILTERB) {
								if(sbgp.toString().contains(fb)) {
									filter = true;
								}
							}
							
							if(!filter && bgps.size()==1 && bgp.vars.size()>0 && bgp.clr.getPartitionCount()==1 && bgpsSeen.add(sbgp)) {
								pw.println(bgp.toTsv()+"\t"+queryString+"\t"+opTypes+"\t"+cols[2]);
								bgpskept ++;
							} else {
								bgpsfiltered ++;
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
		System.out.println("Total BGPs "+(bgpskept+bgpsfiltered));
		System.out.println("Kept BGPs "+bgpskept);
		System.out.println("Filtered BGPs "+bgpsfiltered);
	}
	
	public static ArrayList<OpBGP> getBgps(Op op){
		ArrayList<OpBGP> bgps = new ArrayList<OpBGP>();
		getBgps(op,bgps);
		return bgps;
	}
	
	public static void getBgps(Op op, ArrayList<OpBGP> bgps){
		if(op instanceof OpBGP) {
			bgps.add((OpBGP)op);
		} else if(op instanceof Op1) {
			getBgps(((Op1)op).getSubOp(),bgps);
		} else if(op instanceof Op2) {
			getBgps(((Op2)op).getLeft(),bgps);
			getBgps(((Op2)op).getRight(),bgps);
		} else if(op instanceof OpN) {
			OpN opn = (OpN) op;
			for(Op sop:opn.getElements()) {
				getBgps(sop,bgps);
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
	
	public static class BGP {
		OpBGP opbgp;
		List<Triple> triples;
		int size;

		TreeSet<Node> iris;
		TreeSet<Node> lits;
		TreeSet<Node> vars;

		GraphLabellingResult clr;
		
		ArrayList<Node[]> qgraph;
		ArrayList<Node[]> sgraph;
		
		ArrayList<Node[]> cgraph;
		ArrayList<Node[]> cqgraph;
		
		String signatureVarId;
		String signature;
		
		int treewidth;
		int phtreewidth;
		
		boolean filter = false;
		
		static final Resource S = new Resource("s");
		static final Resource P = new Resource("p");
		static final Resource O = new Resource("o");
		
		static final Resource IRI = new Resource("#");
		static final Resource LIT = new Resource("$");
		static final Resource VAR = new Resource("?");
		
		static final String TPB = "tpb";
		static final Resource[] SPO = { S, P, O };
		
		public BGP(OpBGP opbgp) throws InterruptedException, HashCollisionException {
			this.opbgp = opbgp;
			analyse();
		}
		
		@SuppressWarnings("unchecked")
		public void analyse() throws InterruptedException, HashCollisionException {
			triples = opbgp.getPattern().getList();
			size = triples.size();
			
			sgraph = new ArrayList<Node[]>();
			qgraph = new ArrayList<Node[]>();
			
			iris = new TreeSet<Node>();
			lits = new TreeSet<Node>();
			vars = new TreeSet<Node>();
			
			TreeMap<Node,Integer> varIds = new TreeMap<Node,Integer>();
			
			NGraph<InputData> g = new ListGraph<InputData>();
			NGraph<InputData> phg = new ListGraph<InputData>();
			
			int n = 0;
			for(Triple t: triples) {
				Node[] tn = JenaModelIterator.jenaTripleToNxParser(t);
				convertBnodesVars(tn);
				
				qgraph.add(tn);
				BNode tpb = new BNode(TPB+n);
				ArrayList<Node> tpVars = new ArrayList<Node>();
				
				for(int i=0; i<tn.length; i++) {
					if(tn[i] instanceof Variable) {
						sgraph.add(new Node[] { tpb, SPO[i], new BNode(tn[i].toString()) } );
						vars.add(tn[i]);
						if(!tpVars.contains(tn[i]))
							tpVars.add(tn[i]);
					} else if(tn[i] instanceof Resource) {
						sgraph.add(new Node[] { tpb, SPO[i], IRI } );
						iris.add(tn[i]);
					} else if(tn[i] instanceof Literal) {
						sgraph.add(new Node[] { tpb, SPO[i], LIT } );
						lits.add(tn[i]);
					} else {
						throw new IllegalArgumentException("What kind of node is this? "+tn[i]);
					}
				}
				n++;
				
				if(tpVars.size()>1) {
					Collections.sort(tpVars);
					
					for(int j=0; j<tpVars.size(); j++) {
						Integer varId = varIds.get(tpVars.get(j));
						if(varId == null) {
							varId = varIds.size();
							varIds.put(tpVars.get(j),varId);
							
							NVertex<InputData> v = new ListVertex<InputData>();
							v.data = new InputData();
							v.data.id = varId;
							v.data.name = "v"+varId;
							g.addVertex(v);
							
							NVertex<InputData> phv = new ListVertex<InputData>();
							phv.data = new InputData();
							phv.data.id = varId;
							phv.data.name = "v"+varId;
							phg.addVertex(phv);
						}
					}
					
					for(int j=0; j<tpVars.size()-1; j++) {
						for(int k=j+1; k<tpVars.size(); k++) {
							g.ensureEdge(g.getVertex(varIds.get(tpVars.get(j))),g.getVertex(varIds.get(tpVars.get(k))));
						}
					}
				
					phg.ensureEdge(phg.getVertex(varIds.get(tpVars.get(0))), phg.getVertex(varIds.get(tpVars.get(1))));
					if(tpVars.size()>2) {
						phg.ensureEdge(phg.getVertex(varIds.get(tpVars.get(1))), phg.getVertex(varIds.get(tpVars.get(2))));
					}
				}
			}
			
			treewidth = getTreewidth(g);
			phtreewidth = getTreewidth(phg);
			
			GraphLabelling cl = new GraphLabelling(sgraph);
			clr = cl.call();
			
			HashGraph hg = clr.getHashGraph();
			
			HashMap<Node,HashCode> map = hg.getBlankNodeHashes();
			TreeMap<String,Node> imap = new TreeMap<String,Node>();
			for(Map.Entry<Node, HashCode> mape : map.entrySet()) {
				if(!mape.getKey().toString().startsWith(TPB)) {
					imap.put(mape.getValue().toString(), mape.getKey());
				}
			}
			
			// returns variable mapping in canonical order
			HashMap<Node,Integer> mapn = new HashMap<Node,Integer>();
			int i = 0;
			for(Map.Entry<String, Node> mape : imap.entrySet()) {
				mapn.put(mape.getValue(),i);
				i++;
			}
			
			cgraph = new ArrayList<Node[]>();
			cqgraph = new ArrayList<Node[]>();
			
			for(Node[] qtriple: qgraph) {
				Node[] ctriple = new Node[qtriple.length];
				Node[] cqtriple = new Node[qtriple.length];
				for(int j=0; j<qtriple.length; j++) {
					if(qtriple[j] instanceof Variable) {
						ctriple[j] = new Variable(varChar(mapn.get(new BNode(qtriple[j].toString()))));
						cqtriple[j] = new Variable("v"+mapn.get(new BNode(qtriple[j].toString())));
					} else if (qtriple[j] instanceof Resource) {
						ctriple[j] = IRI;
						cqtriple[j] = qtriple[j];
					} else if (qtriple[j] instanceof Literal) {
						ctriple[j] = LIT;
						cqtriple[j] = qtriple[j];
					}
				}
				cgraph.add(ctriple);
				cqgraph.add(cqtriple);
			}
			
			Collections.sort(cgraph, new NodeComparator());
			Collections.sort(cqgraph, new NodeComparator());
			
			signature = "|";
			signatureVarId = "|";
			for(Node[] ctriple:cgraph) {
				for(int j=0; j<ctriple.length; j++) {
					signatureVarId += ctriple[j].toString();
					
					if(ctriple[j] instanceof Variable) {
						signature += VAR;
					} else {
						signature += ctriple[j].toString();
					}
				}
				signature += "|";
				signatureVarId += "|";
			}
		}
		
		private static void convertBnodesVars(Node[] tn) {
			for(int i=0; i<tn.length; i++) {
				if(tn[i] instanceof Variable) {
					tn[i] = new Variable("v"+tn[i].toString());
				} else if(tn[i] instanceof BNode) {
					tn[i] = new Variable("b"+tn[i].toString());
				}
			}
			
		}

		public static int getTreewidth(NGraph<InputData> g) {
//			MaximumMinimumDegreePlusLeastC<InputData> lbAlgo = new MaximumMinimumDegreePlusLeastC<InputData>();
//			lbAlgo.setInput(g);
//			lbAlgo.run();
//			int lowerbound = lbAlgo.getLowerBound();
//
//			GreedyFillIn<InputData> ubAlgo = new GreedyFillIn<InputData>();
//			ubAlgo.setInput(g);
//			ubAlgo.run();
//			int upperbound = ubAlgo.getUpperBound();
			
			TreewidthDP<InputData> twdp = new TreewidthDP<InputData>();
			twdp.setInput(g);
			twdp.run();
			return twdp.getTreewidth();
		}

		public TreeSet<Node> getVars(){
			return vars;
		}
		
		public TreeSet<Node> getIris(){
			return iris;
		}
		
		public TreeSet<Node> getLits(){
			return lits;
		}
		
		public String toString() {
			return toQueryString(cqgraph);
		}
		
		public String toNonCanonicalisedString() {
			return toQueryString(qgraph);
		}
		
		public String getSignature() {
			return signature;
		}
		
		public String getSignatureVarId() {
			return signatureVarId;
		}
		
		public static String toQueryString(Collection<Node[]> triples) {
			String s = "";
			boolean first = true;
			for(Node[] triple:triples) {
				if(!first) {
					s += " ";
				}
				first = false;
				
				s+=Nodes.toN3(triple);
			}
			return s;
		}
		
		public String toTsv() {
			return clr.getPartitionCount()+"\t"+size+"\t"+iris.size()+"\t"+lits.size()+"\t"+vars.size()+"\t"+treewidth+"\t"+phtreewidth+"\t"+signature+"\t"+signatureVarId+"\t"+toString();
		}
		
		public static String varChar(int i) {
			if(i<10) {
				return Integer.toString(i);
			} else if(i<36) {
				return Character.toString(i+55);
			} else if(i<72) {
				return Character.toString(i+60);
			} else {
				return "'"+i+"'";
			}
		}
	}
	
}
