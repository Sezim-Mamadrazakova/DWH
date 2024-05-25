package de.sparktest
import breeze.linalg.min
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx._
import org.apache.spark.sql.functions.{array, array_contains, array_union, col, collect_set, concat_ws, explode, expr, lit, sort_array, split, struct, udf, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions, functions => F}
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}

import scala.language.postfixOps




object Main {
  def main(args: Array[String]):Unit={
    val DataSource="data/source (1).csv"
    val spark=SparkSession.builder()
      .appName("spark-test")
      .master("local[*]")
      .getOrCreate()
    val df=spark.read
      .option("header", value=true)
      .option("encoding","UTF-8")
      .option("delimiter", value = ";")
      .csv(DataSource)

    val sc=SparkContext.getOrCreate()

    def normalizePhones(phone:String):String= {
      if (phone == null) return ""
      // Удаляем все символы, кроме цифр
      val digitsOnly = phone.replaceAll("\\D", "")
      // Преобразуем в нужный формат
      digitsOnly.length match {
        case 10 => s"+7$digitsOnly" // 10 цифр -> добавляем +7
        case 11 if digitsOnly.startsWith("8") => s"+7${digitsOnly.substring(1)}" // 11 цифр, начинается с 8 -> заменяем 8 на +7
        case 11 if digitsOnly.startsWith("7") => s"+$digitsOnly" // 11 цифр, начинается с 7 -> добавляем +
        case _ => digitsOnly // оставляем как есть, если не соответствует ни одному из условий
      }
    }
    val normalizePhonesUDF: UserDefinedFunction = udf((phones: String) => normalizePhones(phones))
    val normalizedDf = df
      .withColumn("number0", normalizePhonesUDF(col("phone0")))
      .withColumn("number1", normalizePhonesUDF(col("phone1")))
      .withColumn("number2", normalizePhonesUDF(col("phone3")))
      .withColumn("numbers", array("number0", "number1", "number2"))
      .withColumn("numbers", expr("filter(numbers, x -> x != '')"))
      .drop("phone0","phone1","phone3")
    //normalizedDf.show(100)
    //df.show(30)



    normalizedDf.createOrReplaceTempView("df_table")
    //столбец phones{[67868,0],[67868,1]}
    val resultDF = spark.sql(
      """
      SELECT *,
        array(
          struct(number0 AS phone, 0 AS flag),
          struct(number1 AS phone, 1 AS flag),
          struct(number2 AS phone, 2 AS flag)
        ) AS phones
      FROM df_table
      """
    ).withColumnRenamed("serial_number","dul")
      .withColumnRenamed("number0","phone0")
      .withColumnRenamed("number1","phone1")
      .withColumnRenamed("number2","phone3")
    //resultDF.show(false)




    //DataFrame bank
    val dfBank=resultDF
      .filter("client_id >= 1 and client_id <= 800")
      .select(
        col("client_id").as("bank_client_id"),
        col("fio").as("bank_fio"),
        col("dul").as("bank_dul"),
        col("dr").as("bank_dr"),
        col("phones"),
        col("email").as("bank_email")
      )
      .withColumn("bank_system_id",lit(1L))
    //DataFrame insurance
    val dfInsurance=resultDF
      .filter("(client_id >= 100 and client_id <= 300)  " +
        "or (client_id >= 400 and client_id <= 600)"+
      "or (client_id >= 900 and client_id <= 1000)")
      .withColumn("client_id",col("client_id").cast("Integer")+1500)
      .withColumn("dul", when(col("dul").isNotNull, col("dul")).otherwise(col("inn")))
      .withColumn("doc_type", when(col("dul").isNull, lit("ИНН")).otherwise(lit("Паспорт РФ")))
      .select(
        col("client_id").as("ins_client_id"),
        col("fio").as("ins_fio"),
        col("dul").as("ins_dul"),
        col("doc_type").as("doc_type"),
        col("dr").as("ins_dr"),
        col("numbers")
      )
      .withColumn("ins_system_id",lit(2L))
    //DataFrame market
    val dfMarket=resultDF
      .filter("(client_id >= 200 and client_id <= 700)  or (client_id >= 800 and client_id <= 900)")
      .withColumn("client_id",col("client_id").cast("Integer")+3000)
      .select(
        col("client_id").as("market_client_id"),col("email"),col("phone0"),
        split(col("fio")," ").getItem(0).as("surname"),
        split(col("fio")," ").getItem(1).as("first_name"),
        split(col("fio")," ").getItem(2).as("last_name"))
      .withColumn("market_system_id",lit(3L))
      .drop("fio")




    import spark.implicits._

    //  представления
    dfBank.createOrReplaceTempView("bank")
    dfInsurance.createOrReplaceTempView("insurance")


    // SQL-запрос для матчинга
    val query =
          """
         WITH rules AS (
          SELECT * FROM (
            VALUES
              (1, 100),
              (2, 80),
              (3, 70),
              (4,60),
              (5,50),
              (6,40)

          ) AS t(rule_id, weight)
        )
      SELECT
        bank.bank_system_id,
        bank.bank_client_id,
        insurance.ins_system_id,
        insurance.ins_client_id,
        COLLECT_SET(rule_id) as rules,
        MIN(rule_id) as min_rule_id
      FROM
        bank
      CROSS JOIN
        insurance
      JOIN
        rules
      ON
        (CASE WHEN rules.rule_id = 1 THEN bank_fio = ins_fio AND bank_dul = ins_dul AND doc_type="Паспорт РФ" AND bank_dr = ins_dr AND exists(phones, phone -> array_contains(numbers, phone.phone) AND phone.flag = 0)
              WHEN rules.rule_id = 2 THEN bank_fio = ins_fio AND bank_dul = ins_dul AND doc_type="Паспорт РФ" AND bank_dr = ins_dr AND exists(phones, phone -> array_contains(numbers, phone.phone) AND phone.flag = 1)
              WHEN rules.rule_id = 3 THEN bank_fio = ins_fio AND bank_dul = ins_dul AND doc_type="Паспорт РФ" AND bank_dr = ins_dr AND exists(phones, phone -> array_contains(numbers, phone.phone) AND phone.flag = 2)
              WHEN rules.rule_id = 4 THEN bank_fio = ins_fio AND bank_dr = ins_dr AND exists(phones, phone -> array_contains(numbers, phone.phone) AND phone.flag = 0)
              WHEN rules.rule_id = 5 THEN bank_fio = ins_fio AND bank_dr = ins_dr AND exists(phones, phone -> array_contains(numbers, phone.phone) AND phone.flag = 1)
              WHEN rules.rule_id = 6 THEN bank_fio = ins_fio AND bank_dr = ins_dr AND exists(phones, phone -> array_contains(numbers, phone.phone) AND phone.flag = 2)


              ELSE FALSE
        END)
      GROUP BY
        bank.bank_system_id, bank.bank_client_id, insurance.ins_system_id, insurance.ins_client_id
      HAVING
        SIZE(rules) > 0
      """
    val bankAndIns = spark.sql(query)

    println("Связи Банк1 Страховка1")
    //bankAndIns.show(402)
    val finalDF=bankAndIns.drop("rules")



    val pairGraphFD=finalDF.withColumn("edges_pairs",
      functions.concat(col("bank_client_id"),lit(" "),col("ins_client_id"))).select(col("edges_pairs"))

    //edges-разделяем edges_pair. в этом столбце все значения
    val edgesGraphDF=pairGraphFD.withColumn("edges",explode(split(col("edges_pairs")," "))).select("edges")

    //edgesGraphDF.show()
    //преобразование в RDD. создаем вершины графа.
    val profilies:RDD[(VertexId,(String, String))]=sc.parallelize(edgesGraphDF.rdd.map(l => (l.mkString.toLong,("1","1"))).collect())

    //преобразование в RDD. ребра графа.
    val relationships:RDD[Edge[String]]=sc.parallelize(pairGraphFD.rdd.map(l=>Edge(l.mkString.split(" ")(0).toLong,l.mkString.split(" ")(1).toLong,"1")).collect())
    val defaultProfile=("1","1")

    //создаем граф
    val graph=Graph(profilies,relationships,defaultProfile)

    //демонстрация
    graph.connectedComponents().vertices.take(50).foreach(println)

    //родительские вершины, т.е. у них id вершины совпадает с id их родителькой компоненты связанности
    val parentsRDD=graph.connectedComponents().vertices.filter(x=>x._1 == x._2).map(x=>x._1).collect()

    //не родительские вершины
    val vertRDD=graph.connectedComponents().vertices.filter(x=>x._1 != x._2).collect()

    val parentDF=parentsRDD.toList.toDF("parent")
    //parentDF.show(100)
    val vertDF=vertRDD.toList.toDF("from","to")
    //vertDF.show()


   //  функция для создания уникальных идентификаторов графов и их присоединения к основному DF
    def addGraphIds(df: DataFrame, vertDF: DataFrame, parentDF: DataFrame, profileIdCol: String, graphIdCol: String): DataFrame = {
      // Создание уникальных идентификаторов графов
      val uniqueGraphNumberDF = vertDF
        .join(parentDF, vertDF("to") === parentDF("parent"))
        .select(col("parent").as(graphIdCol), col("from").as(profileIdCol))
        .union(parentDF.select(col("parent").as(graphIdCol), col("parent").as(profileIdCol)))

      // Присоединение уникальных идентификаторов графов к основному DataFrame
      val finalDFWithGraphId = df
        .join(uniqueGraphNumberDF, df(profileIdCol) === uniqueGraphNumberDF(profileIdCol), "left")
        .drop(uniqueGraphNumberDF(profileIdCol)) // Удаляем дублирующийся столбец profile_id из uniqueGraphNumberDF
      finalDFWithGraphId
    }


    val finalDFWithGraphId = addGraphIds(bankAndIns, vertDF, parentDF, "bank_client_id", "graph_id").drop("min_rule_id")


    val count1=finalDFWithGraphId.count().toInt
    finalDFWithGraphId.show(count1)



    val dfMarketUpd=dfMarket.withColumn("full_name", concat_ws(" ", dfMarket.col("surname"),dfMarket.col("first_name"),dfMarket.col("last_name")))
    dfMarketUpd.createOrReplaceTempView("market")

    val queryMarketBank =
          """
      WITH rules AS (
        SELECT * FROM (
          VALUES
            (1, 100),
            (2, 80),
            (3, 70),
            (4,60)
        ) AS t(rule_id, weight)
      )
      SELECT
        bank.bank_system_id,
        bank.bank_client_id,
        market.market_system_id,
        market.market_client_id,
        COLLECT_SET(rule_id) as rules,
        MIN(rule_id) as min_rule_id
      FROM
        bank
      CROSS JOIN
        market
      JOIN
        rules
      ON
        (CASE WHEN rules.rule_id = 1 THEN bank_fio = full_name AND bank_email=email  AND exists(phones, phone -> phone.phone=phone0 AND phone.flag = 0)
              WHEN rules.rule_id = 2 THEN bank_fio = full_name AND bank_email=email  AND exists(phones, phone -> phone.phone=phone0 AND phone.flag = 1)
              WHEN rules.rule_id = 3 THEN bank_fio = full_name AND bank_email=email  AND exists(phones, phone -> phone.phone=phone0 AND phone.flag = 2)
              WHEN rules.rule_id = 4 THEN bank_fio = full_name AND bank_email=email
              ELSE FALSE
        END)
      GROUP BY
        bank.bank_system_id, bank.bank_client_id, market.market_system_id, market.market_client_id
      HAVING
        SIZE(rules) > 0
      """
    val bankAndMarket = spark.sql(queryMarketBank)

    val finalBankMarketDF=bankAndMarket.drop("rules")


    val pairGraphBankMarketFD=finalBankMarketDF.withColumn("edges_pairs",
      functions.concat(col("bank_client_id"),lit(" "),col("market_client_id"))).select(col("edges_pairs"))

    //edges-разделяем edges_pair. в этом столбце все значения
    val edgesGraphBankMarketDF=pairGraphBankMarketFD.withColumn("edges",explode(split(col("edges_pairs")," "))).select("edges")

    //edgesGraphDF.show()
    //преобразование в RDD. создаем вершины графа.
    val profiliesBankMarket:RDD[(VertexId,(String, String))]=sc.parallelize(edgesGraphBankMarketDF.rdd.map(l => (l.mkString.toLong,("1","1"))).collect())

    //преобразование в RDD. ребра графа.
    val relationshipsBankMarket:RDD[Edge[String]]=sc.parallelize(pairGraphBankMarketFD.rdd.map(l=>Edge(l.mkString.split(" ")(0).toLong,l.mkString.split(" ")(1).toLong,"1")).collect())


    //создаем граф
    val graph2=Graph(profiliesBankMarket,relationshipsBankMarket,defaultProfile)

    //демонстрация
    graph2.connectedComponents().vertices.take(50).foreach(println)

    //родительские вершины, т.е. у них id вершины совпадает с id их родителькой компоненты связанности
    val parentsBankMarketRDD=graph2.connectedComponents().vertices.filter(x=>x._1 == x._2).map(x=>x._1).collect()

    // вершины
    val vertBankMarketRDD=graph2.connectedComponents().vertices.filter(x=>x._1 != x._2).collect()

    val parentBankMarketDF=parentsBankMarketRDD.toList.toDF("parent")
    //parentDF.show(100)
    val vertBankMarketDF=vertBankMarketRDD.toList.toDF("from","to")
    //vertDF.show()

    val finalDFWithGraphId2 = addGraphIds(bankAndMarket, vertBankMarketDF, parentBankMarketDF, "bank_client_id", "graph_id").drop("min_rule_id")
    val count2=finalDFWithGraphId2.count().toInt
    finalDFWithGraphId2.show(count2)


























  }

}
