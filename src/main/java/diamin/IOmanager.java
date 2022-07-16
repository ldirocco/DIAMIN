package diamin;

import diamin_scala.graphMiner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

public class IOmanager {


    public static BioGraph importFromTxt(String path, boolean header, String sep, int[] indices, JavaSparkContext jsc){
        JavaRDD<String> edgesRDD = jsc.textFile(path);

        if(header){
            String head=edgesRDD.first();
            edgesRDD=edgesRDD.filter(line->!line.equals(head));
        }

        JavaRDD<Row> edges_to_attributes=edgesRDD
                .filter(line->{
                    String[] vertices=line.split(sep);
                    return !vertices[0].equals(vertices[1]);})
                .map(line->{
                    String[] attributes=line.split(sep);
                    Object[] edge_attributes=new Object[3];
                    int counter=0;
                    for(int i:indices){
                        edge_attributes[counter]=attributes[i];
                        counter ++;
                    }
                    Row r= RowFactory.create(edge_attributes);
                    return r;
                });


        StructType edges_attributes_structure = new StructType()
                .add(new StructField("src", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("dst", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("weight", DataTypes.StringType, true, Metadata.empty()));

        SQLContext sqlContext = new SQLContext(jsc.sc());
        Dataset<Row> edgesDF=sqlContext.createDataFrame(edges_to_attributes,edges_attributes_structure);

        //graphUtils.create_graph(edgesDF);
        GraphFrame ppi=GraphFrame.fromEdges(edgesDF);

        return new BioGraph(jsc,ppi);
    }

    public static BioGraph importFromNeo4j(SparkSession spark, String url, String user, String password,String propRef,String condition) {
        GraphFrame graph=GraphFrame.fromEdges(graphMiner.edgesFromNeo4j(spark, url, user, password,propRef,condition));
        JavaSparkContext jsc=new JavaSparkContext(spark.sparkContext());
        return new BioGraph(jsc,graph);
    }

    public static void exportToTsv(BioGraph MIN, String filename) {
        MIN.interactions().coalesce(1).write()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true").option("delimiter", "\t").save(filename);
    }

    public static void toNeo4j(BioGraph MIN,String url,String user,String password){
        graphMiner.graphToNeo4J(MIN.interactions(),url,user,password);
    }
}

