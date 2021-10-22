package cl.uchile.wikidata.query;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.jena.graph.Node_Blank;
import org.apache.jena.graph.Node_Literal;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.graph.Node_Variable;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;

/**
 * Provides a way to use Jena models as input to blabel.
 * 
 * Note that the source-code is commented out to avoid having to include
 * bulky Jena dependencies. If you want to use blabel as part of your project
 * you can just comment the code back in. :)
 * 
 * @author Aidan
 *
 */

public class JenaModelIterator implements Iterator<Node[]> {
	final StmtIterator iter;
	
	public JenaModelIterator(Model m){
		iter = m.listStatements();
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public Node[] next() {
		if(!hasNext())
			throw new NoSuchElementException();
		
		return jenaStatementToNxParser(iter.next());
	}

	@Override
	public void remove() {
		iter.remove();
	}
	
	public static Node[] jenaStatementToNxParser(Statement s){
		return new Node[] { jenaTermToNxParser(s.getSubject().asNode()),
				jenaTermToNxParser(s.getPredicate().asNode()),
				jenaTermToNxParser(s.getObject().asNode()) };
	}
	
	public static Node[] jenaTripleToNxParser(Triple t){
		return new Node[] { jenaTermToNxParser(t.getSubject()),
				jenaTermToNxParser(t.getPredicate()),
				jenaTermToNxParser(t.getObject()) };
	}
	
	
	public static org.semanticweb.yars.nx.Node jenaTermToNxParser(
			org.apache.jena.graph.Node jenaNode) {
		if (jenaNode instanceof Node_URI)
			return new Resource(jenaNode.getURI(), false);
		else if (jenaNode instanceof Node_Blank) {
			String bNlabel = jenaNode.getBlankNodeLabel();
			char firstChar = bNlabel.charAt(0);
			if (firstChar <= 57 && firstChar >= 48) {
				// starts with a number ... should be a letter
				bNlabel = "j" + bNlabel;
			}

			// encodes strings into valid bnode label
			return BNode.createBNode(bNlabel);
		}
		else if (jenaNode instanceof Node_Literal) {
			String lang = jenaNode.getLiteralLanguage() == null ? null : 
							jenaNode.getLiteralLanguage().equals("") ? null : 
								jenaNode.getLiteralLanguage();
			
			Resource datatype = null;
			if(lang==null) {
				datatype = jenaNode.getLiteralDatatypeURI() == null ? null : 
							jenaNode.getLiteralDatatypeURI().equals("") ? null : 
								new Resource(jenaNode.getLiteralDatatypeURI(),false);
			}
			return new Literal(jenaNode.getLiteralLexicalForm(),lang,datatype);
		} else if (jenaNode instanceof Node_Variable) {
			return new Variable(jenaNode.getName());
		} else
			throw new UnsupportedOperationException("Unknown Jena node type "+jenaNode.getClass().getName());

	}
}
