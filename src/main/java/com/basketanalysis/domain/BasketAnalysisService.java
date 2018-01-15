package com.basketanalysis.domain;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import com.basketanalysis.entities.SegmentedBasketItem;
import com.basketanalysis.repositories.AssociationRulesRepository;
import com.basketanalysis.repositories.BasketItemRepository;

import scala.Console;

public class BasketAnalysisService {
	
	public void calculateAssociationRules() throws SQLException {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("BasketAnalysis");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		Connection connection = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;databaseName=BasketAnalysis","sa", "Password1");
		connection.setAutoCommit(false);
		
		BasketItemRepository basketItemRepositor = new BasketItemRepository();
		
		ArrayList<SegmentedBasketItem> segmentedBasketItems = basketItemRepositor.getSegmentedBasketItems(connection);
		
		for ( SegmentedBasketItem segmentedBasketItem : segmentedBasketItems) {
			
			ArrayList<String> dbTransactions = basketItemRepositor.getTransactions(connection, segmentedBasketItem);
			JavaRDD<String> rddTransactions = sc.parallelize(dbTransactions);
			Console.print(dbTransactions.size());

			JavaRDD<List<String>> basketTransactions = rddTransactions.map(line -> Arrays.asList(line.split(" ")));

			FPGrowthModel<String> model = generateFrequentItemsets(dbTransactions, basketTransactions);
			
			generateAssociationRules(connection, segmentedBasketItem, model);
			
		}

		connection.close();
		sc.stop();
		sc.close();
	}

	private void generateAssociationRules(Connection connection, SegmentedBasketItem segmentedBasketItem, FPGrowthModel<String> model) throws SQLException {
		AssociationRulesRepository associationRulesRepository = new AssociationRulesRepository();
		
		associationRulesRepository.deleteRules(connection, segmentedBasketItem);
		double minConfidence = 0.8;
		int rulesCount = 0;
		for (AssociationRules.Rule<String> rule
				: model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
			associationRulesRepository.insertRules(rule, connection, segmentedBasketItem);
			System.out.println(
					rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
			rulesCount++;
		}
		Console.print(rulesCount);
	}

	private FPGrowthModel<String> generateFrequentItemsets(ArrayList<String> dbTransactions,
			JavaRDD<List<String>> basketTransactions) {
		double minSupport = calculateMinSupport(dbTransactions.size());
		FPGrowth fpg = new FPGrowth()
				.setMinSupport(0.2)
				.setNumPartitions(10);
		FPGrowthModel<String> model = fpg.run(basketTransactions);

		for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
			System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
		}
		return model;
	}
	
	private static double calculateMinSupport(int transactionsCount) {
		double a = 0.002;
		double b = 0.001;
		double minSupport = Math.exp((-a * transactionsCount) - b) + 0.0005;
		
		return minSupport;
	}
}
