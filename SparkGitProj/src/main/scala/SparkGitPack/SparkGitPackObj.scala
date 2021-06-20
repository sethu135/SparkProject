package SparkGitPack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkGitPackObj {
  
  def main(args:Array[String]):Unit={
	  
	  val conf=new SparkConf().setAppName("TASKS").setMaster("local")
	  val sc=new SparkContext(conf)
	  sc.setLogLevel("ERROR")
	  
	  val spark=new SparkSession.Builder().getOrCreate()
	  import spark.implicits._
	  
	  val input_data=spark.read.format("json").option("multiLine","true").load("file:///E:/Hadoop/INPUT/MultiArrays.json")
	  println
	  println
	  println("========================RAW INPUT DATA==================================")
	  input_data.printSchema()
	  input_data.show(10,false)
	  
	  val flat_data_exp_1=input_data.withColumn("Students", explode(col("Students")))
	  
	  println
	  println
	  println("========================EXPLODE STUDENTS==================================")
	  flat_data_exp_1.printSchema()
	  flat_data_exp_1.show(10,false)
	  
	   val flat_data_exp_2=flat_data_exp_1.withColumn("components", explode(col("Students.user.components")))
	  
	  println
	  println
	  println("========================EXPLODE COMPONENTS==================================")
	  flat_data_exp_2.printSchema()
	  flat_data_exp_2.show(10,false)
	  
	  
	  val flat_data_final=flat_data_exp_2.select(
	      
	      col("Students.user.address.Permanent_address").as("C_Permanent_address"),
	       col("Students.user.address.temporary_address").as("C_temporary_address"),
	       col("Students.user.gender"),
	       col("Students.user.name.first"),
	       col("Students.user.name.last"),
	       col("Students.user.name.title"),
	       col("address.Permanent_address"),
         col("address.temporary_address"),
	       col("first_name"),
	       col("second_name"),
	       col("components")
	  )
	  
	  
	  println
	  println
	  println("========================FINAL FLATTENED DATA==================================")
	  flat_data_final.printSchema()
	  flat_data_final.show(10,false)
	  
	  
	  val raw_data_step1=flat_data_final.groupBy("Permanent_address", "temporary_address","first_name","second_name").agg(
	      
	      collect_list
	      (
	      
	      struct
	      (
	          struct
	          (
	              struct
	              (
	                  col("C_permanent_address").as("Permanent_address"),
	                  col("C_temporary_address").as("temporary_address")
	                  ).as("address"),
	                   array("components").as("componenets"),
	                   col("gender"),
	                  struct
	                  (
	                      col("first"),
	                      col("last"),
	                      col("title")
	                   ).as("name")
	              
	                   
	              ).as("user")
	         
	      )
	      ).as("Students")
	      
	      )
	      
	      println
	      println
	      println("=========================FLATTEN TO RAW DATA BACK=====================")
	      raw_data_step1.show(5,false)
	      raw_data_step1.printSchema()
	 // val raw_data_step2=raw_data_step1.select
	}
  
}