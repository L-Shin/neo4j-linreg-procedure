package example;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.List;


public class Similarity {

    @UserFunction
    @Description("example.cosine([1.2, 2.2], [3.2, 2.1]) - compute cosine distance for the given lists of ratings")
    public Double cosine(@Name("set1") List<Double> seta, @Name("set2") List<Double> setb) {

        Double dot = 0.0;
        Double ssa = 0.0;
        Double ssb = 0.0;

        for (int i = 0; i < seta.size(); i++) {
            dot += seta.get(i) * setb.get(i);
            ssa += Math.pow(seta.get(i), 2);
            ssb += Math.pow(setb.get(i), 2);
        }

        ssa = Math.pow(ssa, 0.5);
        ssb = Math.pow(ssb, 0.5);

        return dot / (ssa * ssb);
    }
}


