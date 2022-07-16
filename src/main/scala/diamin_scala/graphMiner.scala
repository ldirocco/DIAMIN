package diamin_scala

import java.util
import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object graphMiner {

  def dijkstra(g: Graph[Row, Row], p:String):Double = {
    val input_graph
    = g.mapVertices((_, attr) => {
      val vertex = if (p.equals(attr.getString(0)))  0.0 else Double.PositiveInfinity
      vertex
    })

    val distance = input_graph.pregel(Double.PositiveInfinity,activeDirection=EdgeDirection.Either)(
      (_, vertex, newDist) => {
        val minDist = math.min(vertex, newDist)
        minDist
      },
      triplet => {
        if (triplet.srcAttr +1 < triplet.dstAttr ) {
          Iterator((triplet.dstId, triplet.srcAttr+ 1))
        } else if(triplet.dstAttr + 1 < triplet.srcAttr){
          Iterator((triplet.srcId, triplet.dstAttr + 1))
        } else {
          Iterator.empty
        }
      },
      (dist1, dist2) => {
        val minDist = math.min(dist1,dist2)
        minDist
      }
    ).vertices.values.filter(t=>t>0).map(t=>math.pow(t,-1)).reduce((t1,t2)=>t1+t2)


    distance
  }

  def xNeighbors(g: Graph[Row, Row], p:String, x:Int):RDD[Row]={
    val input_graph = g.mapVertices((_, attr) => {
      val in_condition = if (p.equals(attr.getString(0)))  true else false
      (attr.getString(0),in_condition)
    })

    val output_graph = input_graph.pregel(false,x,activeDirection=EdgeDirection.Either)(
      (_, vertex, newDist) => {(vertex._1,vertex._2 || newDist)},
      triplet => {
        if (triplet.srcAttr._2 && !triplet.dstAttr._2) {
          Iterator((triplet.dstId, true))
        } else if(triplet.dstAttr._2 && !triplet.srcAttr._2){
          Iterator((triplet.srcId, true))
        } else {
          Iterator.empty
        }
      },
      (dist1, dist2) => dist2 || dist1
    )

    output_graph.vertices.values.filter(t=>t._2 && !t._1.equals(p)).map(t=>Row(t._1,t._2.toString))
  }




  def xSubGraph(g: Graph[Row, Row], interactors:util.ArrayList[String], x:Int):RDD[String]= {
    val input_graph = g.mapVertices((_, attr) => {
      val vertex = (attr.getString(0), interactors.contains(attr.getString(0)))
      vertex
    })

    val output_graph = input_graph.pregel(false,x,activeDirection=EdgeDirection.Either)(
      (_, vertex, newDist) => {(vertex._1,vertex._2 || newDist)},
      triplet => {
        if (triplet.srcAttr._2 && !triplet.dstAttr._2) {
          Iterator((triplet.dstId, true))
        } else if(triplet.dstAttr._2 && !triplet.srcAttr._2){
          Iterator((triplet.srcId, true))
        } else {
          Iterator.empty
        }
      },
      (dist1, dist2) => dist2 || dist1
    )

    output_graph.vertices.values.filter(t=>t._2).map(t=>t._1)

  }

  def xWeightedNeighbors(g: Graph[Row, Row], p:String, x:Int):RDD[Row]={
    val input_graph = g.mapVertices((_, attr) => {
      val vertex = (attr.getString(0), if (attr.getString(0) == p) 1 else -1.0)
      vertex
    })

    val output_graph = input_graph.pregel(-1.0,activeDirection=EdgeDirection.Either)(
      (_, attr, newDist) => (attr._1,Math.max(newDist,attr._2)),
      triplet => {
        //Distance accumulator
        val combined_score=triplet.attr.getString(2).toDouble
        if (combined_score*triplet.srcAttr._2>x&&combined_score*triplet.srcAttr._2!=triplet.dstAttr._2&&triplet.dstAttr._2!=1) {
          Iterator((triplet.dstId, Math.abs(combined_score*triplet.srcAttr._2)))
        } else if(combined_score*triplet.dstAttr._2>x&&combined_score*triplet.dstAttr._2!=triplet.srcAttr._2&&triplet.srcAttr._2!=1){
          Iterator((triplet.srcId, Math.abs(combined_score*triplet.dstAttr._2)))
        } else {
          Iterator.empty
        }
      },
      (a, b) => Math.max(a,b)
    )

    output_graph.vertices.values.filter(t=>t._2>=x && !t._1.equals(p)).map(t=>Row(t._1,t._2.toString))
  }


  def xWeightedSubgraph(g: Graph[Row, Row], interactors:util.ArrayList[String], x:Int):RDD[String]= {
    val input_graph = g.mapVertices((_, attr) => {
      val vertex = (attr.getString(0), if (interactors.contains(attr.getString(0))) 1 else -1.0)
      vertex
    })

    val output_graph = input_graph.pregel(-1.0,activeDirection=EdgeDirection.Either)(
      (_, attr, newDist) => (attr._1,Math.max(newDist,attr._2)),
      triplet => {
        //Distance accumulator
        val combined_score=triplet.attr.getString(2).toDouble
        if (combined_score*triplet.srcAttr._2>x&&combined_score*triplet.srcAttr._2!=triplet.dstAttr._2&&triplet.dstAttr._2!=1) {
          Iterator((triplet.dstId, Math.abs(combined_score*triplet.srcAttr._2)))
        } else if(combined_score*triplet.dstAttr._2>x&&combined_score*triplet.dstAttr._2!=triplet.srcAttr._2&&triplet.srcAttr._2!=1){
          Iterator((triplet.srcId, Math.abs(combined_score*triplet.dstAttr._2)))
        } else {
          Iterator.empty
        }
      },
      (a, b) => Math.max(a,b)
    )

    output_graph.vertices.values.filter(t=>t._2>x).map(t=>t._1)
  }


  def edgesFromNeo4j(spark:SparkSession,url:String,user:String,password:String,propRef:String,condition:String): DataFrame ={
    var wCondition=""
    var properties=""

    if(!condition.equals("")){
      wCondition="WHERE "+condition.split(",")(0)
      for(i<-1 to condition.split(",").length-1){
        wCondition+=" AND "+condition.split(",")(i)+" "
      }
    }

    if(!propRef.equals("")){
      val propList=propRef.split(",")
      for(i<-0 to propList.length-1){
        properties+=",r."+propList(i)
      }
    }
    val query="MATCH (a:protein)-[r]->(b:protein) RETURN a.name AS src,b.name AS dst"

    val output=spark.read.format("org.neo4j.spark.DataSource")
      .option("url",url)
      .option("authentication.basic.username",user)
      .option("authentication.basic.password",password)
      .option("query",query)
      .load()

    return output

  }


  def graphToNeo4J(df:DataFrame,url:String,user:String,password: String)={
    val propertiesArray=df.drop("src").drop("dst").columns
    var properties=propertiesArray(0);
    for(i<-1 to propertiesArray.length-1){
      properties+=","+propertiesArray(i)
    }
    df.write
      .format("org.neo4j.spark.DataSource")
      .option("url", url)
      .option("authentication.basic.username",user)
      .option("authentication.basic.password",password)
      .option("relationship", "INTERACTION")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":protein")
      .option("relationship.source.save.mode", "overwrite")
      .option("relationship.source.node.keys", "src:name")
      .option("relationship.target.labels", ":protein")
      .option("relationship.target.node.keys", "dst:name")
      .option("relationship.target.save.mode", "overwrite")
      .option("relationship.properties", properties)
      .save()
  }
}
