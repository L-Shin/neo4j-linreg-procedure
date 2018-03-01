package example;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import org.apache.commons.math3.stat.regression.SimpleRegression;


import static org.neo4j.helpers.collection.MapUtil.stringMap;

/** In this class I will create two implementations
 * of a linear regression model.
 *
 * 1. Create a user defined function in which the user manually inputs
 * the slope/intercept coefficients obtained through a third
 * party data analysis, and then can call this function on the graph
 * to predict unknown values.
 *
 * 2. Create a user defined procedure in which the user specifies which
 * node type and what properties will be the independent and dependent values
 * of the regression. The procedure will then create a linear regression
 * model using all nodes with known x and y values and map that model onto all nodes
 * with known x but unknown y values, storing the predicted y value as another
 * property. The linear regression model will be stored as an isolated node so that
 * it can be updated as more known data is added to the graph.
 */

public class LinearRegression {


    @UserFunction
    @Description("example.predict(b, m, x) - uses the regression parameters intercept = b, coefficient = m, " +
            "and returns a predicted y value based on the equation y = m * x + b")
    public Double predict(@Name("intercept") Double intercept, @Name("slope") Double slope, @Name("input")
            Double input) {
        return (slope * input) + intercept;
    }

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;


    @Procedure(value = "example.simpleRegression", mode = Mode.WRITE)
    @Description("create a linear regression model using independent and dependent property data from nodes that have" +
            " the given label and contain both properties. Then store predicted values under the property name " +
            "'newVarName' for nodes with the same label and known x but no known y property value")

    public void simpleRegression(@Name("label") String label, @Name("indpendent variable") String indVar,
                                 @Name("dependent variable") String depVar, @Name("new variable name") String newVarName) {
        SimpleRegression R = new SimpleRegression();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("label", label);
        parameters.put("indVar", indVar);
        parameters.put("depVar", depVar);
        //build the model using indVar and depVar data from nodes with label
        Result resultKnown = db.execute("MATCH (node:$label) WHERE exists(node.$indVar) AND exists(node.$depVar) RETURN node",
                parameters);
        ResourceIterator<Node> knownNodes = resultKnown.columnAs("node");
        while (knownNodes.hasNext()) {
            Node curr = knownNodes.next();
            Object x = curr.getProperty(indVar);
            Object y = curr.getProperty(depVar);
            // TODO: 3/1/18 deal with error if properties are not numbers
            if (x instanceof Number && y instanceof Number) {
                R.addData((double) x, (double) y);
            }

        }
        //predict depVar values
        Result resultUnknown = db.execute("MATCH (node:$label) WHERE exists(node.$indVar) AND NOT exists(node.$depVar) RETURN node", parameters);
        ResourceIterator<Node> unknownNodes = resultUnknown.columnAs("node");
        while (unknownNodes.hasNext()) {
            Node curr = unknownNodes.next();
            Object x = curr.getProperty(indVar);
            // TODO: 3/1/18 deal with error if properties are not numbers
            if (x instanceof Number) {
                curr.setProperty(newVarName, R.predict((double) x));
            }

        }

    }
}



