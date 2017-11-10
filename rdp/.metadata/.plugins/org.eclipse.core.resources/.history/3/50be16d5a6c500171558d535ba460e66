package br.com.igti.data.rdp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PrimeiroProgramaSpark {
   
    public static void main(String[] args) {

        // configuração do Spark
        SparkConf conf = new SparkConf()
                                  .setMaster("local")
                                  .setAppName("programa");
        JavaSparkContext ctx = new JavaSparkContext(conf);
              
        // carrega os dados do arquivo
        JavaRDD<String> linhas = ctx.textFile("data/cap1-tweets.txt");
        long numeroLinhas = linhas.count();
              
        // escreve o número de linhas que existem no arquivo
        System.out.println(numeroLinhas);

        // encerra o contexto da aplicação
        ctx.close();
    }
}
