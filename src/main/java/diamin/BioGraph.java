package diamin;

import diamin_scala.graphMiner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
/***
 * Models in a Java application the concept of a distributed MIN.
***/

public class BioGraph {
    private JavaSparkContext jsc;
    private GraphFrame MIN;

    public BioGraph(JavaSparkContext jsc, GraphFrame MIN){
        this.jsc=jsc;
        this.MIN=MIN;
        jsc.sc().setCheckpointDir("Diamin-CheckpointDir");
    }

    /**
     * Returns the number of interactors (i.e., the vertices) of the underlying graph.
     * @return |V| - number of interactors (vertices).
     */
    public long interactorsCounter(){return MIN.vertices().count();}
    /**
     * Returns the number of interactions (i.e., the edges) of the underlying graph.
     * @return |E| - number of interactors (edges).
     */
    public long interactionsCounter(){return MIN.edges().count();}

    /**
     * Writes on a file the unique id or the name of all the interactors (i.e., the nodes) of the underlying graph.
     * @param filename Name of the output file.
     */
    public void interactors(String filename) {
        //return just one file
        MIN.vertices().coalesce(1).write().format("csv").option("header", false).mode("overwrite").option("sep", ",").save(filename);
    }

    /**
     * Writes on a file the unique id or the name of all interactions (i.e., edges) of the underlying graph.
     * @param filename Name of the output file.
     */
    public void interactions(String filename){
        //return just one file
        MIN.edges().coalesce(1).write().format("csv").option("header",false).mode("overwrite").option("sep",",").save(filename);
    }



    //public Dataset<Row> interactions(){
    //    return MIN.edges();
    //}


    /**
     * Computes the density index of the underlying graph.
     * @return d - density index.
     */
    public float density(){
        float nVertices=(float)MIN.vertices().count();
        float nEdges=(float)MIN.edges().count();
        float density=2*nEdges/(nVertices*(nVertices-1));
        return density;
    }

    /**
     * Returns the list of interactors of the underlying graph, with their associated degree.
     *
     * @param n Number of the top-scored interactors to return.
     *
     * @return The list of tuples <interactor_id, degree>.
     */
    public ArrayList<Tuple2<String, Integer>> degrees(int n){
        return MIN.degrees();
    }
    //public Row[] degrees(int n){
    //    return MIN.degrees().sort("degree").take(n);
    //}
    //public Dataset<Row> inDegrees(){
    //    return MIN.inDegrees();
    //}
    //public Dataset<Row> outDegrees(){
    //    return MIN.outDegrees();
    //}


    /**
     * Returns the x-neighbors of a subset  S of interactors
     * @param S Set of interactors.
     * @param x Length of the path.
     * @return The list of x-neighbors.
     */
    public List<String> xNeighbors(String S, int x){
        JavaRDD<Row> xNeighbors_rdd=graphMiner.xNeighbors(MIN.toGraphX(),S,x).toJavaRDD();

        StructType neighbors_attributes_structure = new StructType()
                .add(new StructField("id", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("x", DataTypes.StringType, true, Metadata.empty()));

        SQLContext sqlContext = new SQLContext(jsc.sc());
        Dataset<Row> edgesDF=sqlContext.createDataFrame(xNeighbors_rdd,neighbors_attributes_structure);
        return edgesDF.toJavaRDD().map(t->t.getString(0)).collect();
    }

    /**
     * Returns the subgraph containing a subset S of interactors and their x-neighbors.
     * @param S Set of interactors.
     * @param x Length of the path.
     * @return The BioGraph storing the subgraph of x-neighbors.
     */
    public BioGraph xSubGraph(String S, int x) {
        ArrayList<String> interactors = new ArrayList<>();
        for (String interactor : S.split(",")) {
            interactors.add(interactor);
        }
        List<String> xNeighbors_list = graphMiner.xSubGraph(MIN.toGraphX(), interactors, x).toJavaRDD().collect();

        for(String s:xNeighbors_list){
            System.out.println(s);
        }

        JavaRDD<Row> xSubgraph_rdd=MIN.edges().toJavaRDD().filter(t-> xNeighbors_list.contains(t.getString(0)) && xNeighbors_list.contains(t.getString(1)));

        StructType neighbors_edges_structure = new StructType()
                .add(new StructField("src", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("dst", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("weight", DataTypes.StringType, true, Metadata.empty()));

        SQLContext sqlContext = new SQLContext(jsc.sc());
        Dataset<Row> edgesDF=sqlContext.createDataFrame(xSubgraph_rdd,neighbors_edges_structure);

        GraphFrame xsubgraphframe=GraphFrame.fromEdges(edgesDF);

        BioGraph xsubgraph=new BioGraph(jsc,xsubgraphframe);

        return xsubgraph;
    }


    /**
     * Returns the x-weighted-neighborhood of an input interactor.
     * @param i Interactor Name.
     * @param x Minimum threshold.
     * @return The list of x-weighted-neighborhood of i.
     */
    public List<String> xWeightedNeighbors(String i, int x){
        JavaRDD<Row> xNeighbors_rdd=graphMiner.xWeightedNeighbors(MIN.toGraphX(),i,x).toJavaRDD();

        StructType neighbors_attributes_structure = new StructType()
                .add(new StructField("id", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("x", DataTypes.StringType, true, Metadata.empty()));

        SQLContext sqlContext = new SQLContext(jsc.sc());
        Dataset<Row> edgesDF=sqlContext.createDataFrame(xNeighbors_rdd,neighbors_attributes_structure);
        return edgesDF.toJavaRDD().map(t->t.getString(0)).collect();
    }

    /***
     * Returns the subgraph containing an input interactor and its x-weighted-neighbors.
     * @param i Interactor Name.
     * @param x Minimum threshold.
     * @return The BioGraph storing the subgraph of x-weighted neighbors.
     */

    public BioGraph xWeightedSubgraph(String i, int x) {
        ArrayList<String> interactors = new ArrayList<>();
        for (String interactor : i.split(",")) {
            interactors.add(interactor);
        }
        List<String> xNeighbors_list = graphMiner.xWeightedSubgraph(MIN.toGraphX(), interactors, x).toJavaRDD().collect();

        for(String s:xNeighbors_list){
            System.out.println(s);
        }

        JavaRDD<Row> xSubgraph_rdd=MIN.edges().toJavaRDD().filter(t-> xNeighbors_list.contains(t.getString(0)) && xNeighbors_list.contains(t.getString(1)));

        StructType neighbors_edges_structure = new StructType()
                .add(new StructField("src", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("dst", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("weight", DataTypes.StringType, true, Metadata.empty()));

        SQLContext sqlContext = new SQLContext(jsc.sc());
        Dataset<Row> edgesDF=sqlContext.createDataFrame(xSubgraph_rdd,neighbors_edges_structure);

        GraphFrame xsubgraphframe=GraphFrame.fromEdges(edgesDF);

        BioGraph xsubgraph=new BioGraph(jsc,xsubgraphframe);

        return xsubgraph;
    }

    /**
     * Returns the connected component of the underlying graph, containing the largest number of elements in common with an input subset of interactors.
     * @param S su
     * @return
     */
    public Dataset<Row> closestComponents(String S){
        ArrayList<Object> interactors=new ArrayList<>();
        for(String i:S.split(",")){
            interactors.add(i);
        }

        Dataset<Row> components = MIN.connectedComponents().run();

        Tuple2<Long, Integer> max = components.javaRDD()
                .mapToPair(r -> new Tuple2<>(Long.parseLong(r.getString(1)), r.get(0)))
                .mapToPair(t->{
                    if(interactors.contains(t._2)) {
                        return new Tuple2<Long,Integer>(t._1,1);
                    } else {
                        return new Tuple2<Long,Integer>(t._1,0);
                    }
                })
                .reduceByKey(Integer::sum)
                .max((t1,t2)->{
                    if(t1._2>=t2._2) {
                        return 1;
                    }
                    return -1;
                });

        return components.filter("component=" + max._1).select("id");
    }
    
}
