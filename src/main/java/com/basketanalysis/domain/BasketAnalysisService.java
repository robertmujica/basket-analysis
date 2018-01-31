package com.basketanalysis.domain;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import com.basketanalysis.entities.FrequentItemset;
import com.basketanalysis.entities.FrequentItemsetsResponse;
import com.basketanalysis.entities.SegmentedBasketItem;
import com.basketanalysis.repositories.AssociationRulesRepository;
import com.basketanalysis.repositories.BasketItemRepository;

import scala.Console;

public class BasketAnalysisService {
	
	public void calculateAssociationRules() throws SQLException, Exception {
		PrintStream fileStream = new PrintStream("analysis.log");
		System.setOut(fileStream);
		long startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setMaster("local").setAppName("BasketAnalysis");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		Connection connection = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;databaseName=BasketAnalysis","sa", "Password1");
		connection.setAutoCommit(false);
		
		BasketItemRepository basketItemRepositor = new BasketItemRepository();
		
		ArrayList<SegmentedBasketItem> segmentedBasketItems = basketItemRepositor.getSegmentedBasketItems(connection);
		
		int totalRules = 0;
		for ( SegmentedBasketItem segmentedBasketItem : segmentedBasketItems) {
			
			
			ArrayList<String> dbTransactions = basketItemRepositor.getTransactions(connection, segmentedBasketItem);
			
			System.out.println("****************  Generating rules for: " + segmentedBasketItem.getRegion() + "- " + segmentedBasketItem.getCountry() + " - " +  
					segmentedBasketItem.getLanguage() + " - " 
					+ segmentedBasketItem.getSalesSegment() + " - " + segmentedBasketItem.getCmsSegment() + 
					" Total transactions :" + dbTransactions.size() + "   ********************");
			
			JavaRDD<String> rddTransactions = sc.parallelize(dbTransactions);

			JavaRDD<List<String>> basketTransactions = rddTransactions.map(line -> Arrays.asList(line.split(" ")));

			FrequentItemsetsResponse freqItemsetResponse = generateFrequentItemsets(dbTransactions, basketTransactions);
			
			totalRules += generateAssociationRules(connection, segmentedBasketItem, freqItemsetResponse);
			System.out.println("");
		}

		connection.close();
		sc.stop();
		sc.close();
		long endTime = System.currentTimeMillis();
		Console.println("Total execution time: " + ((endTime - startTime)/1000) + " Total Rules : " + totalRules);
	}

	private int generateAssociationRules(Connection connection, SegmentedBasketItem segmentedBasketItem, FrequentItemsetsResponse freqItemsetResponse) throws SQLException {
		AssociationRulesRepository associationRulesRepository = new AssociationRulesRepository();
		
		associationRulesRepository.deleteRules(connection, segmentedBasketItem);
		double minConfidence = 0.4;
		int rulesCount = 0;
		
		System.out.println("Generating Association Rules - Min Confidence : " + minConfidence);
		for (AssociationRules.Rule<String> rule
				: freqItemsetResponse.getModel().generateAssociationRules(minConfidence).toJavaRDD().collect()) {
			
			Long itemsetFrequency = freqItemsetResponse.getFrequentItemsets().get(rule.javaAntecedent().hashCode());
			if(itemsetFrequency != null) {
				associationRulesRepository.insertRules(rule, connection, segmentedBasketItem, itemsetFrequency);
			}
			
			System.out.println(
					rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
			rulesCount++;
		}
		return rulesCount;
	}

	private FrequentItemsetsResponse generateFrequentItemsets(ArrayList<String> dbTransactions,
			JavaRDD<List<String>> basketTransactions) {
		
		double minSupport = 0.2; //scalculateMinSupport(dbTransactions.size());
		FPGrowth fpg = new FPGrowth()
				.setMinSupport(minSupport);
		
		FPGrowthModel<String> model = fpg.run(basketTransactions);
		Hashtable<Integer, Long> frequentItemsets = new Hashtable<Integer, Long>();

		System.out.println("Generating Frequent Itemsets - Min Support: " + minSupport);
		for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
			frequentItemsets.put(itemset.javaItems().hashCode(), itemset.freq());
			System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
		}
		
		return new FrequentItemsetsResponse(frequentItemsets, model);
	}
	
	private static double calculateMinSupport(int transactionsCount) {
		double a = 0.001;
		double b = 0.002;
		double minSupport = Math.exp((-a * transactionsCount) - b);
		
		return minSupport;
	}
}
