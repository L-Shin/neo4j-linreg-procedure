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

public class SimilarityTest {

    // Start a Neo4j instance
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFunction(Similarity.class);

    @Test
    public void shouldCalculateCosineDistance() throws Throwable {

        // Create a driver session, and run Cypher query
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {
            Session session = driver.session();

            Double result = session
                    .run("RETURN example.cosine([1.2, 3.4], [3.2, 1.3]) AS result")
                    .single().get("result").asDouble();

            assertThat(result, equalTo(0.6632666323374395));
        }
    }
}



