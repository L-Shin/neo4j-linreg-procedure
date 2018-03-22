package example;

import java.util.HashMap;
import java.util.Map;
import java.io.*;


import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import org.apache.commons.math3.stat.regression.SimpleRegression;



/** In this class I will create three implementations
 * of linear regression.
 *
 * 1. Create a user defined function in which the user manually inputs
 * the slope/intercept coefficients obtained through a third
 * party data analysis tool, and then calls this function on the graph
 * to predict unknown values.
 *
 * 2. Create a user defined procedure in which the user specifies which
 * node/relationship type and properties of that entity will be the x and y values
 * of the regression. The procedure will then create a linear regression
 * model using all entities with known x and y values and map that model onto all entities
 * with known x but unknown y values, storing the predicted y value as another
 * property. Store serialized model object in a new node.
 *
 * 3. Create a user defined procedure in which the user provides one query that will return entities with properties
 * used to create the model, and another query that will return entities on which the model will predict unknown values.
 * Store serialized model object in a new node.
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
             throw new RuntimeException("Invalid dataSource (acceptable values are 'node' or 'relationship')");
        }

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("label", label);
        parameters.put("indVar", indVar);
        parameters.put("depVar", depVar);

        //gathers points with known indVar and depVar
        Result resultKnown;

        //gathers points with known indVar but no known depVar
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

        //build the model
        while (knownValues.hasNext()) {
            Entity curr = knownValues.next();
            Object x = curr.getProperty(indVar);
            Object y = curr.getProperty(depVar);

            if (x instanceof Number && y instanceof Number) {
                R.addData((double) x, (double) y);
            }

        }

        if (R.getN() < 2) {
            throw new RuntimeException("not enough known values to create a model");
        }

        //predict depVar values
        ResourceIterator<Entity> unknownValues = resultUnknown.columnAs(dataSource);
        while (unknownValues.hasNext()) {
            Entity curr = unknownValues.next();
            Object x = curr.getProperty(indVar);

            if (x instanceof Number) {
                curr.setProperty(newVarName, R.predict((double) x));
            }
        }
        parameters.put("int", R.getIntercept());
        parameters.put("slope", R.getSlope());

        ResourceIterator<Entity> modelNode = db.execute("CREATE (n:LinReg {label:$label, indVar:$indVar, depVar:$depVar, " +
                "intercept:$int, slope:$slope}) RETURN n", parameters).columnAs("n");
        Entity n = modelNode.next();

        //Serialize R and store as property "serializedModel" in the new LinReg node
        try (ByteArrayOutputStream model = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(model)){
            out.writeObject(R);
            byte[] byteModel = model.toByteArray();
            n.setProperty("serializedModel", byteModel);

        } catch (IOException e) {

        }
    }

    /* modelQuery must return a Result that contains a column titled indVar and a column titled depVar. This data will be used
    to create the model. mapQuery must return a single column result of type Entity (node or relationship). If entries in this
    column contain the property indVar and not the property depVar, the predicted depVar value will be stored under the property
    named newVarName
     */
    @Procedure(value = "example.customRegression", mode = Mode.WRITE)
    @Description("Create a linear regression model using the the two data points which result from running the modelQuery." +
            "Then store predicted values on the Entities that result from running the mapQuery.")
    public void customRegression(@Name("model query") String modelQuery, @Name("map query") String mapQuery,
                                 @Name("independent variable") String indVar, @Name("dependent variable") String depVar,
                                 @Name("new variable name") String newVarName) {
        Result knownValues;

        try
        {
            knownValues = db.execute(modelQuery);
        }
        catch (QueryExecutionException q)
        {
            throw new RuntimeException("model query is invalid");
        }

        SimpleRegression R = new SimpleRegression();
        if (!(knownValues.columns().contains(indVar)&&knownValues.columns().contains(depVar))) {
            throw new RuntimeException("model query returns data with invalid column titles-must match indVar and depVar");
        }
        while(knownValues.hasNext()) {
            Map<String, Object> row = knownValues.next();
            R.addData((double) row.get(indVar), (double) row.get(depVar));
        }
        if (R.getN()<2) {
            throw new RuntimeException("not enough data to create a model");
        }
        Result r;
        String columnTitle;
        try
        {
            r = db.execute(mapQuery);
            columnTitle = r.columns().get(0);
        }
        catch (QueryExecutionException q)
        {
            throw new RuntimeException("map query is invalid");
        }

        ResourceIterator<Entity> unknowns = r.columnAs(columnTitle);

        while(unknowns.hasNext()) {
            Entity e = unknowns.next();
            if (e.hasProperty(indVar) && !e.hasProperty(depVar)) {
                e.setProperty(newVarName, R.predict((double) e.getProperty(indVar)));
            }

        }
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("indVar", indVar);
        parameters.put("depVar", depVar);
        parameters.put("int", R.getIntercept());
        parameters.put("slope", R.getSlope());

        ResourceIterator<Entity> modelNode = db.execute("CREATE (n:LinReg:Custom {indVar:$indVar, depVar:$depVar, " +
                "intercept:$int, slope:$slope}) RETURN n", parameters).columnAs("n");
        Entity n = modelNode.next();

        //Serialize R and store as property "serializedModel" in the new LinReg node
        try (ByteArrayOutputStream model = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(model)){
            out.writeObject(R);
            byte[] byteModel = model.toByteArray();
            n.setProperty("serializedModel", byteModel);

        } catch (IOException e) {

        }
    }
}



