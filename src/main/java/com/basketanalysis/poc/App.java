package com.basketanalysis.poc;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.AssociationRules.Rule;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import com.basketanalysis.domain.BasketAnalysisService;
import com.basketanalysis.entities.SegmentedBasketItem;
import com.basketanalysis.repositories.AssociationRulesRepository;
import com.basketanalysis.repositories.BasketItemRepository;

import scala.Console;

public class App 
{
	public static void main( String[] args ) throws Exception
	{
		BasketAnalysisService basketAnalysisService = new BasketAnalysisService();
		basketAnalysisService.calculateAssociationRules();
		//sparkSample();
	}

	private static void sparkSample() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("FP-growth Example");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// $example on$
		JavaRDD<String> data = sc.textFile("data/sample_fpgrowth.txt");

		JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.split(" ")));

		FPGrowth fpg = new FPGrowth()
				.setMinSupport(0.2)
				.setNumPartitions(10);
		FPGrowthModel<String> model = fpg.run(transactions);

		for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
			System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
		}

		double minConfidence = 0.8;
		for (AssociationRules.Rule<String> rule
				: model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
			System.out.println(
					rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
		}
		// $example off$

		sc.stop();
	}
}
