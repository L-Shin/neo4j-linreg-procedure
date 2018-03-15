package example;

import java.util.HashMap;
import java.util.Map;


import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import org.apache.commons.math3.stat.regression.SimpleRegression;



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

    //single variable linear regression using node or relationship properties
    @Procedure(value = "example.simpleRegression", mode = Mode.WRITE)
    @Description("create a linear regression model using independent and dependent property data from nodes/relationships that have" +
            " the given label and contain both properties. Then store predicted values under the property name " +
            "'newVarName' for nodes/relationships with the same label and known x but no known y property value. " +
            "Store the linear regression coefficients in a new LinReg node. Use of nodes vs relationships specified with dataSource")

    public void simpleRegression(@Name("label") String label, @Name("independent variable") String indVar,
                                 @Name("dependent variable") String depVar, @Name("new variable name") String newVarName,
                                 @Name("data source") String dataSource) {

        if (!(dataSource.equals("node")||dataSource.equals("relationship"))) {
            // TODO: error
        }

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("label", label);
        parameters.put("indVar", indVar);
        parameters.put("depVar", depVar);

        //build the model using indVar and depVar data from nodes with label
        Result resultKnown;

        Result resultUnknown;

        if (dataSource.equals("node")) {
            resultKnown = db.execute("MATCH (node) WHERE $label IN labels(node) AND $indVar IN keys(node) AND $depVar IN keys(node) " +
                    "RETURN DISTINCT node", parameters);
            resultUnknown = db.execute("MATCH (node) WHERE $label IN labels(node) AND $indVar IN keys(node) AND NOT $depVar IN keys(node) RETURN DISTINCT node", parameters);

        } else  {

            resultKnown = db.execute("MATCH () - [r] - () WHERE type(r) = $label AND $indVar IN keys(r) AND $depVar IN keys(r)" +
                            "RETURN DISTINCT r AS relationship", parameters);
            resultUnknown = db.execute("MATCH () - [r] - () WHERE type(r) = $label AND $indVar IN keys(r) AND NOT $depVar IN keys(r)" +
                    "RETURN DISTINCT r as relationship", parameters);
        }

        ResourceIterator<Entity> knownValues = resultKnown.columnAs(dataSource);
        SimpleRegression R = new SimpleRegression();

        while (knownValues.hasNext()) {
            Entity curr = knownValues.next();
            Object x = curr.getProperty(indVar);
            Object y = curr.getProperty(depVar);
            // TODO: 3/1/18 deal with error if properties are not numbers
            if (x instanceof Number && y instanceof Number) {
                R.addData((double) x, (double) y);
            }

        }
        //predict depVar values

        ResourceIterator<Entity> unknownValues = resultUnknown.columnAs(dataSource);
        while (unknownValues.hasNext()) {
            Entity curr = unknownValues.next();
            Object x = curr.getProperty(indVar);
            // TODO: 3/1/18 deal with error if properties are not numbers
            if (x instanceof Number) {
                curr.setProperty(newVarName, R.predict((double) x));
            }
        }
        parameters.put("int", R.getIntercept());
        parameters.put("slope", R.getSlope());

        db.execute("CREATE (n:LinReg {nodeLabel:$label, indVar:$indVar, depVar:$depVar, intercept:$int, slope:$slope})", parameters);

    }


}



