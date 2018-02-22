package example;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.equalTo;

public class LinearRegressionTest {

    // Start a Neo4j instance
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFunction(Similarity.class);

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

            // TODO: 2/22/18 add test for procedure which creates nodes of same label, some with two float properties and some with only one. Check slope, intercept, and new calculated property values
        }
    }
}

