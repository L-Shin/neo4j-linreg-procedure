package example;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.equalTo;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.HashMap;

public class LinearRegressionTest {

    // Start a Neo4j instance
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFunction(LinearRegression.class);

    @Test
    public void shouldPredictValues() throws Throwable {

        // Create a driver session, and run Cypher query
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {
            Session session = driver.session();

            Double result = session
                    .run("RETURN example.predict(3.9, 0.453, 2) AS result")
                    .single().get("result").asDouble();

            assertThat(result, equalTo(0.453*2 + 3.9));
        }
    }

    @Test
    public void shouldCreateLinearRegression() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {
            Session session = driver.session();

            // TODO: 3/1/18 do this in a better way. Maybe create a graph from a csv file? 
            session.run("CREATE (node {time:1, progress:1.345})");
            session.run("CREATE (node {time:2, progress:2.596})");
            session.run("CREATE (node {time:3, progress:3.259})");
            session.run("CREATE (node {time:4})");
            session.run("CREATE (node {time:5})");
            session.run("CALL example.simpleRegression(['node'], ['time'], ['progress'], ['predictedProgress'])");
            StatementResult result = session.run("MATCH (n:node) WHERE exists(n.predictedProgress) RETURN n.time AS time, n.predictedProgress AS predictedProgress");

            SimpleRegression R = new SimpleRegression();
            R.addData(1.0, 1.345);
            R.addData(2.0, 2.596);
            R.addData(3.0, 3.259);

            HashMap<Double, Double> expected= new HashMap<>();
            expected.put(4.0, R.predict(4));
            expected.put(5.0, R.predict(5));


            while (result.hasNext()) {
                Record actual = result.next();
                assertEquals(expected.get(actual.get("time").asDouble()), (Double) actual.get("predictedProgress").asDouble());
            }



            // TODO: 2/22/18 add test for procedure which creates nodes of same label, some with two float properties and some with only one. Check slope, intercept, and new calculated property values
        }
    }
}

