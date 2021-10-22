package cl.uchile.wikidata.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpNull;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.Rename;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.aggregate.Aggregator;

public class RemoveServiceClauses extends TransformCopy {
    @Override
    public Op transform(OpJoin join, Op left, Op right) {
        if (left instanceof OpService && right instanceof OpService) { 
        	// could remove this in a second pass, but joins
        	//  between services should be rare I guess
        	return OpNull.create();
        }

        if (left instanceof OpService) {
        	return right;
        }
        
        if(right instanceof OpService) {
        	return left;
        }
        
        return join;
    }

	@Override
	public Op transform(OpGroup opGroup, Op subOp) {
    	Set<Var> visibleVars = OpVars.visibleVars(subOp);
    	List<ExprAggregator> aggList = opGroup.getAggregators();

    	Op op = new OpGroup(subOp, opGroup.getGroupVars(), opGroup.getAggregators());
    	
    	// variables used in aggregators (e.g., inside count, etc.)
    	for(ExprAggregator expr:aggList) {
    		Aggregator agg = expr.getAggregator();
    		if(agg != null) {
    			ExprList el = expr.getAggregator().getExprList();
    			if(el != null) {
    				for(Var aVar: el.getVarsMentioned()) {
        	    		if(!visibleVars.contains(aVar)) {
        	    			if(aVar.toString().endsWith("Label")) {
        	    				String newVarName = aVar.toString().substring(1,aVar.toString().length()-5);
        	    				op = Rename.renameVar(op, aVar, Var.alloc(newVarName));
        	    			}
        	    		}
    				}
    			}
    		}
    	}
    	
    	OpGroup opg = (OpGroup) op;
    	HashSet<Var> setVars = new HashSet<Var>();
    	VarExprList vel = new VarExprList();
    	
    	// variables used for grouping (need to avoid creating dupes)
    	for(Var gVar: opg.getGroupVars().getVars()) {
    		Var newVar = gVar;
    		if(!visibleVars.contains(gVar)) {
    			if(gVar.toString().endsWith("Label")) {
    				String newVarName = gVar.toString().substring(1,gVar.toString().length()-5);
    				newVar = Var.alloc(newVarName);
    			}
    		}
    		
    		if(setVars.add(newVar)) {
    			vel.add(newVar);
    		}
    	}
    	
    	for(Map.Entry<Var, Expr> ve: opg.getGroupVars().getExprs().entrySet()) {
    		// this *might* cause problems as I'm not 100% sure
    		//   how group by handles non-variable expressions, so
    		//     just copying them, possible with ?varLabel type variables
    		//        though they should probably be rewritten higher up
    		if(setVars.add(ve.getKey())) {
    			vel.add(ve.getKey(), ve.getValue());
    		}
    	}
    	
    	return new OpGroup(opg.getSubOp(), vel, opg.getAggregators());
	}

	@Override
	public Op transform(OpProject opProject, Op subOp) {
		Set<Var> visibleVars = OpVars.visibleVars(subOp);
    	List<Var> projectVariables = opProject.getVars();
    	
    	ArrayList<Var> listVars = new ArrayList<Var>();
    	Op op = new OpProject(subOp, listVars);
    	
    	Set<Var> setVars = new HashSet<Var>();
    	
    	for(Var projectVariable:projectVariables) {
    		Var newVar = projectVariable;
    		if(!visibleVars.contains(projectVariable)) {
    			if(projectVariable.toString().endsWith("Label")) {
    				String newVarName = projectVariable.toString().substring(1,projectVariable.toString().length()-5);
    				newVar = Var.alloc(newVarName);
    				try {
    					op = Rename.renameVar(op, projectVariable, newVar);
    				} catch(Exception e) {
    					System.err.println("\n\n"+op);
    					throw e;
    				}
    			}
    		}
    		if(setVars.add(newVar)) {
    			listVars.add(newVar);
    		}
    	}
    	
    	return new OpProject(((OpProject)op).getSubOp(), listVars);
	}
	
	public static Op transformTopLevel(Op op) {
		Set<Var> visibleVars = OpVars.visibleVars(op);
		Collection<Var> mentionedVars = OpVars.mentionedVars(op);
    	
		Op opx = op;
		for(Var mentionedVar:mentionedVars) {
			if(!visibleVars.contains(mentionedVar)) {
				if(mentionedVar.toString().endsWith("Label")) {
    				String newVarName = mentionedVar.toString().substring(1,mentionedVar.toString().length()-5);
    				Var newVar = Var.alloc(newVarName);
    				opx = Rename.renameVar(opx, mentionedVar, newVar);
    			}
			}
		}
    	
		return opx;
	}
}
