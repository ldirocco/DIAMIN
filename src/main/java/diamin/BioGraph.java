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

public class BioGraph {
    private JavaSparkContext jsc;
    private GraphFrame MIN;

    public BioGraph(JavaSparkContext jsc, GraphFrame MIN){
        this.jsc=jsc;
        this.MIN=MIN;
        jsc.sc().setCheckpointDir("Diamin-CheckpointDir");
    }

    public long interactorsCounter(){return MIN.vertices().count();}
    public long interactionsCounter(){return MIN.edges().count();}
    public void interactors(String filename) {
        //return just one file
        MIN.vertices().coalesce(1).write().format("csv").option("header", false).mode("overwrite").option("sep", ",").save(filename);
    }
    public void interactions(String filename){
        //return just one file
        MIN.edges().coalesce(1).write().format("csv").option("header",false).mode("overwrite").option("sep",",").save(filename);
    }

    public Dataset<Row> interactions(){
        return MIN.edges();
    }


    public float density(){
        float nVertices=(float)MIN.vertices().count();
        float nEdges=(float)MIN.edges().count();
        float density=2*nEdges/(nVertices*(nVertices-1));
        return density;
    }

    public Dataset<Row> degrees(){
        return MIN.degrees();
    }
    public Row[] degrees(int n){
        return MIN.degrees().sort("degree").take(n);
    }
    public Dataset<Row> inDegrees(){
        return MIN.inDegrees();
    }
    public Dataset<Row> outDegrees(){
        return MIN.outDegrees();
    }


    public List<String> xNeighbors(String p, int x){
        JavaRDD<Row> xNeighbors_rdd=graphMiner.xNeighbors(MIN.toGraphX(),p,x).toJavaRDD();

        StructType neighbors_attributes_structure = new StructType()
                .add(new StructField("id", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("x", DataTypes.StringType, true, Metadata.empty()));

        SQLContext sqlContext = new SQLContext(jsc.sc());
        Dataset<Row> edgesDF=sqlContext.createDataFrame(xNeighbors_rdd,neighbors_attributes_structure);
        return edgesDF.toJavaRDD().map(t->t.getString(0)).collect();
    }

    public BioGraph xSubGraph(String interactors_string, int x) {
        ArrayList<String> interactors = new ArrayList<>();
        for (String interactor : interactors_string.split(",")) {
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


    public List<String> xWeightedNeighbors(String p, int x){
        JavaRDD<Row> xNeighbors_rdd=graphMiner.xWeightedNeighbors(MIN.toGraphX(),p,x).toJavaRDD();

        StructType neighbors_attributes_structure = new StructType()
                .add(new StructField("id", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("x", DataTypes.StringType, true, Metadata.empty()));

        SQLContext sqlContext = new SQLContext(jsc.sc());
        Dataset<Row> edgesDF=sqlContext.createDataFrame(xNeighbors_rdd,neighbors_attributes_structure);
        return edgesDF.toJavaRDD().map(t->t.getString(0)).collect();
    }

    public BioGraph xWeightedSubgraph(String interactors_string, int x) {
        ArrayList<String> interactors = new ArrayList<>();
        for (String interactor : interactors_string.split(",")) {
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

    public Dataset<Row> closestComponents(String input_interactors){
        ArrayList<Object> S=new ArrayList<>();
        for(String i:input_interactors.split(",")){
            S.add(i);
        }

        Dataset<Row> components = MIN.connectedComponents().run();

        Tuple2<Long, Integer> max = components.javaRDD()
                .mapToPair(r -> new Tuple2<>(Long.parseLong(r.getString(1)), r.get(0)))
                .mapToPair(t->{
                    if(S.contains(t._2)) {
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

    public void intersectionByComponent(){

    }



}
