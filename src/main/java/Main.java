import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.poi.poifs.nio.DataSource;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.data.jpa.convert.threeten.Jsr310JpaConverters;
//import org.springframework.data.jpa.repository.JpaRepository;
//import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.time.LocalDate;
import java.util.Date;
import java.util.Properties;


public class Main {
    private static final String number = "number";
    private static final String date = "date";

    public static void main(String[] args) throws Exception {
        System.out.println("hi");

        //build the spark sesssion
        SparkSession session = SparkSession.builder().appName("data in excel").master("local[*]").getOrCreate();

        //set the log level only to log errors
        session.sparkContext().setLogLevel("ERROR");

        //define schema type of file data source
        StructType schema = new StructType().add("name", DataTypes.StringType).add("date", DataTypes.DateType)
                .add("docnum", DataTypes.DoubleType);

        //build the streaming data reader from the file source, specifying excel file format
        //todo excel file
//        Dataset<Row> fileStreamDF = session.read().format("com.crealytics.spark.excel")
//                .option("location", "in/Book1.xlsx")
//                .option("useHeader", "true")
//                .schema(schema)
//                .option("treatEmptyValuesAsNulls", "true")
//                .option("inferSchema", true)
//                .option("addColorColumns", "False")
//                .load();

        System.out.println(new Date());
        //read from csv file
        Dataset<Row> fileStreamDF = session.read()
                .option("header", "true")
                .format("csv")
                .schema(schema)
                .csv("C:/Users/Acer/IdeaProjects/spark_project/in/*.csv");
        System.out.println(fileStreamDF.isStreaming());
        fileStreamDF.printSchema();

//        Dataset<Row> trimmedDF = fileStreamDF.select(fileStreamDF.col("name"), fileStreamDF.col("date"));
//        StreamingQuery query = trimmedDF.writeStream()
//                .outputMode("append")
//                .format("console")
//                .option("truncate", "false")
//                .option("numRows", 30)
//                .start();
//        query.awaitTermination();


        System.out.println("*********** save into database *************");
        fileStreamDF.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/sparktestdb")
                .option("dbtable", "sparktestdb4")
                .option("user", "root")
                .option("password", "1234")
                .save();
        System.out.println(new Date());
//
//        System.out.println("*********** load from database *************");
//        Dataset<Row> dbDataSet = session.read()
//                .format("jdbc")
//                .option("url", "jdbc:mysql://localhost:3306/sparktestdb")
//                .option("dbtable", "sparktestdb2")
//                .option("user", "root")
//                .option("password", "1234").load();


//        System.out.println("********** show on console *********");
//        dbDataSet.write()
//                .format("console")
//                .option("truncate", "false")
//                .option("numRows", 30)
//                .save();

//        System.out.println("********** save in csv file *************");
//        dbDataSet.write()
//                .format("csv")
//                .csv("C:/Users/Acer/IdeaProjects/spark_project/out/result");


//        Dataset<Row> dataset = session.read()
//                .format("jdbc")
//                .option("url", "jdbc:mysql://localhost:3306/sparktestdb")
//                .option("dbtable", "sparktestdb")
//                .option("user", "root")
//                .option("password", "1234")
//                .load();
//        Dataset<Row> dataset1 = dataset.withColumn(number, dataset.col(number).cast("long"));
////        dataset1.show();
//        session.sql("select * from sparktestdb").show();

        //        Dataset<Row> dataset = session.read().option("header", "true").csv("in/data.txt");
//        Dataset<Row> dataset2 = dataset.withColumn(number, dataset.col(number).cast("double")).withColumn(date, dataset.col(date));
//        dataset2.show();

    }
}
