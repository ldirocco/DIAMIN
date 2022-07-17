# DIAMIN
_DIAMIN_ is a high-level software library to facilitate the development of distributed applications for the efficient analysis of large-scale molecular interaction networks.

NOTE: an extended version of this documentation is included in [Manual.pdf](https://github.com/ldirocco/DIAMIN/blob/master/Manual.pdf) and the source code documentation is available in the [diamin_JavaDoc](https://ldirocco.github.io/javadoc/).

## Usage
The library runs over Apache Spark (>=2.3,https://spark.apache.org/) and requires a Java compliant virtual machine (>= 1.8). 
It is released as a jar file, [diamin-1.0.0-all.jar](https://github.com/ldirocco/DIAMIN/releases), and can be used to develop applications running either in Spark Local Mode or Spark Cluster Mode. Additionally, this jar file contains a sample application useful to analyze an input provided molecular interaction network using some of the functions implemented by _DIAMIN_.

We refer to the following links for information about the installation of a Java compliant virtual machine, version 1.8, as required by our library:

- [JDK 8 Installation for Windows](https://docs.oracle.com/javase/8/docs/technotes/guides/install/windows_jdk_install.html#CHDEBCCJ)
- [JDK 8 Installation for Linux](https://docs.oracle.com/javase/8/docs/technotes/guides/install/linux_jdk.html#BJFGGEFG)
- [JDK 8 Installation for OS X](https://docs.oracle.com/javase/8/docs/technotes/guides/install/mac_jdk.html#CHDBADCG)


## Running the sample application

The diamin-1.0.0-all.jar includes a sample application useful to analyze an input provided molecular interaction network using some of the functions implemented by _DIAMIN_. It is possible to execute it from the command-line using the following syntax:


```
java -jar diamin-1.0.0-all.jar [_input_parameters_] function_name [input_parameters]
```

where:

- **_input_parameters:_** the path to the molecular interaction network to be analyzed;
- **_function_name:_** the _DIAMIN_ function to run
- **_function_parameters:_** the list of the parameters, if any, required by the chosen function.

Notice that, by default, the _DIAMIN_ sample application runs using the Spark local mode. This means that the application is run on a single machine, but uses, in parallel, all the processing cores available on that machine. Instructions on how to run this application in fully distributed environment are available at the end of this text.

In the following, we report some usage examples of this application. We refer the interested reader of the _DIAMIN_ usage the [Manual.pdf](https://github.com/ldirocco/DIAMIN/blob/master/Manual.pdf) for an exhaustive list of all the functions included in the library. 

#### Example 1
In a Molecular Interaction Network, pivotal interactors are likely to be represented by highly connected nodes (i.e., hubs). 
The _degree_ function of the _DIAMIN_ library allows the user to extract a subset of interactors, according to the value of their degree. 
This function computes the degrees of each interactor and it returns all those elements satisfying a given condition.

In the following example, the _degree_ function is used to compute the interactors of the Homo Sapiens INTACT nework associated with the 20 largest degrees:
```
java -jar diamin-1.0.0-all.jar human_intact_network.txt degree 20
```
#### Example 2

The analysis of neighborhoods of specific nodes in a MIN is a very common task in the literature. Given an input interactor _i_, the function _xWeightedNeighbors_ provided by the _DIAMIN_ library returns the neighbors of _i_ such that the product of the labels of the connecting edges is greater than x.

In the following example, the _xWeightedNeighbors_ function is used to compute the x-weighted_neighborhood of the protein TP53 (uniprotkb:P04637) 
w.r.t. the Intact reliability scores for x=0.75:

```
java -jar diamin-1.0.0-all.jar human_intact_network.txt xWeightedNeighbors uniprotkb:P04637,0.75
```

### Running the Application on an Apache Spark Distributed Cluster

Being coded using Apache Spark, the _DIAMIN_ library is able to run in a distributed environment, thus taking advantage of the many processing cores potentially available in this setting. The same applies to the _DIAMIN_ sample application. Assuming both Apache Spark and Java are properly installed, 
it is possible to run the code implementing Example 2 on the front-end node of an Apache Spark computing cluster using the following syntax
```
spark-submit diamin-1.0.0-all.jar CLUSTER human_intact_network.txt degree 20
```

Notice that, in this case, the application is run using the *spark-submit* command rather than the *java* command. Moreover, differently from the previous examples, here the _CLUSTER_ option is needed to tell _DIAMIN_ to run its code using the underlying computing cluster. 


## Developing new applications using DIAMIN

The primary usage for DIAMIN is as a software library aimed to simplify the development of distributed applications for the efficient analysis of large-scale molecular interaction networks. In the following we describe the example of the development of a Java application for the evaluation of the Kleinberg dispersion measure on an input molecular network. 


### Example: Kleinberg dispersion computation.
Intuitively, the Kleinberg dispersion quantifies how _not well-connected_ is the  common neighborhood of two interactors _u_ and _v_ in a Molecular Interaction Network. It takes into account both the size and the connectivity of  the common neighborhood of _u_ and _v_. In the following, we provide the Java code that implements the Kleinberg dispersion computation using the _DIAMIN_ library. A step-by-step description of the code is provided in the [Manual.pdf](https://github.com/ldirocco/DIAMIN/blob/master/Manual.pdf).

  ```java

import diamin.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setAppName("DIAMIN").setMaster("local[8]");
        sc.set("spark.local.dir", "tmp");
        sc.set("spark.executor.memory", "2g");
        JavaSparkContext jsc = new JavaSparkContext(sc);

        String interactors="uniprotkb:P04637,uniprotkb:P06422";
        String input_path="intact_graph.txt";
        BIOgraph MIN=IOmanager.importFromtxt(input_path,jsc);
        String[] C_uv=MIN.xNeighbors(interactors,1);
        BIOgraph C_u=MIN.xSubgraph("uniprotkb:P04637");
        int kleinberg_dispersion=0;
        for(String s:C_uv){
          for(String t:C_uv){
            int n_neighbors=C_u.xNeighbors(s+","+t,2).length;
            if(n_neighbors>0){kleinberg_dispersion+=1;}
           }
        }
        System.out.println(kleinberg_dispersion);
    }
}

  ```






## Datasets
The _DIAMIN_ library was tested by using the following protein-to-protein networks:
- [IntAct database](http://www.ebi.ac.uk/intact): it is an open-source, open data molecular interaction database populated by data either curated from the literature or from direct data depositions. Its graph-based representation contains 116,641 nodes and 768,993 edges.
- [String database](https://string-db.org): it is a database aiming to integrate all known and predicted associations between proteins, including both physical interactions as well as functional associations. To achieve this, STRING collects and scores evidence from a number of sources. Its graph-based representation contains 19,354 nodes and  11,759,454 edges.
