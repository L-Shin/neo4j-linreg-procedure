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
            .withFunction(LinearRegression.class)
            .withProcedure(LinearRegression.class);

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
    public void shouldCreateNodeRegression() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {
            Session session = driver.session();


            session.run("CREATE (node {time:1.0, progress:1.345})");
            session.run("CREATE (node {time:2.0, progress:2.596})");
            session.run("CREATE (node {time:3.0, progress:3.259})");
            session.run("CREATE (node {time:4.0})");
            session.run("CREATE (node {time:5.0})");
            session.run("CALL example.simpleRegression('node', 'time', 'progress', 'predictedProgress', 'node')");
            StatementResult result = session.run("MATCH (n:node) WHERE exists(n.predictedProgress) RETURN n.time, n.predictedProgress");

            SimpleRegression R = new SimpleRegression();
            R.addData(1.0, 1.345);
            R.addData(2.0, 2.596);
            R.addData(3.0, 3.259);

            HashMap<Double, Double> expected= new HashMap<>();
            expected.put(4.0, R.predict(4.0));
            expected.put(5.0, R.predict(5.0));


            while (result.hasNext()) {
                Record actual = result.next();

                double time = actual.get("time").asDouble();
                double expectedPrediction = expected.get(time);
                double actualPrediction = actual.get("predictedProgress").asDouble();

                assertThat(actualPrediction, equalTo(expectedPrediction));
            }




        }
    }
    @Test
    public void shouldCreateRelRegression() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {
            Session session = driver.session();

            session.run("CREATE (:Node {id:1}) - [:WORKS_FOR {time:1.0, progress:1.345}] -> " +
                    "(:Node {id:2}) - [:WORKS_FOR {time:2.0, progress:2.596}] -> " +
                    "(:Node {id:3}) - [:WORKS_FOR {time:3.0, progress:3.259}] -> (:Node {id:4})");
            session.run("CREATE (:Node {id:5}) -[:WORKS_FOR {time:4.0}] -> (:Node {id:6}) - [:WORKS_FOR {time:5.0}] -> (:Node {id:7})");
            session.run("CALL example.simpleRegression('WORKS_FOR', 'time', 'progress', 'predictedProgress', 'relationship')");
            StatementResult result = session.run("MATCH () - [r:WORKS_FOR] - () WHERE exists(r.time) AND exists(r.predictedProgress) RETURN r.time as time, r.predictedProgress as predictedProgress");

            SimpleRegression R = new SimpleRegression();
            R.addData(1.0, 1.345);
            R.addData(2.0, 2.596);
            R.addData(3.0, 3.259);

            HashMap<Double, Double> expected = new HashMap<>();
            expected.put(4.0, R.predict(4.0));
            expected.put(5.0, R.predict(5.0));

            while (result.hasNext()) {
                Record actual = result.next();

                double time = actual.get("time").asDouble();
                double expectedPrediction = expected.get(time);
                double actualPrediction = actual.get("predictedProgress").asDouble();

                assertThat( actualPrediction, equalTo( expectedPrediction ) );


            }
        }
    }
}

