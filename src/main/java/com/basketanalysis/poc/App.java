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

import scala.Console;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void main( String[] args ) throws SQLException
	{
		//sparkSample();
		sqlConnection();
	}

	public static void sqlConnection() throws SQLException {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("BasketAnalysis");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		Connection connection = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;databaseName=BasketAnalysis","sa", "Password1");
		connection.setAutoCommit(false);
		ArrayList<String> dbTransactions = getTransactions(connection);
		JavaRDD<String> rddTransactions = sc.parallelize(dbTransactions);
		Console.print(dbTransactions.size());

		JavaRDD<List<String>> basketTransactions = rddTransactions.map(line -> Arrays.asList(line.split(" ")));

		double a = 0.002;
		double b = 0.001;
		double minSupport = Math.exp((-a * dbTransactions.size()) - b) + 0.005;
		FPGrowth fpg = new FPGrowth()
				.setMinSupport(minSupport)
				.setNumPartitions(10);
		FPGrowthModel<String> model = fpg.run(basketTransactions);

		for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
			System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
		}
		
		deleteRules(connection, "US", "bsd");
		double minConfidence = 0.8;
		int rulesCount = 0;
		for (AssociationRules.Rule<String> rule
				: model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
			insertRules(rule, connection);
			System.out.println(
					rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
			rulesCount++;
		}
		Console.print(rulesCount);

		connection.close();
		sc.stop();
	}

	private static void deleteRules(Connection connection, String country, String cmsSegment) throws SQLException {
		
		try {
			// remove detail records
			PreparedStatement detailSql = connection.prepareStatement("delete [dbo].[AssociationRulesDetail]\r\n" + 
					"  where AssociationRulesMasterId in (\r\n" + 
					"  select Id from dbo.AssociationRulesMaster\r\n" + 
					"  where Country = ? and CmsSegment = ?)");
			
			detailSql.setString(1,  country);
			detailSql.setString(2, cmsSegment);
			detailSql.executeUpdate();
			
			// remove master records
			PreparedStatement masterSql = connection.prepareStatement("DELETE FROM [dbo].[AssociationRulesMaster]\r\n" + 
					"      WHERE Country = ? and CmsSegment = ?");
			
			masterSql.setString(1,  country);
			masterSql.setString(2, cmsSegment);
			masterSql.executeUpdate();
			
			connection.commit();
		} catch (SQLException e) {
			connection.rollback();
			e.printStackTrace();
		}
	}

	private static void insertRules(Rule<String> rule, Connection connection) throws SQLException {
		PreparedStatement sql;
		try {
			sql = connection.prepareStatement("INSERT INTO [dbo].[AssociationRulesMaster]\r\n" + 
					"           ([Region]\r\n" + 
					"           ,[Country]\r\n" + 
					"           ,[Language]\r\n" + 
					"           ,[SalesSegment]\r\n" + 
					"           ,[CmsSegment]\r\n" + 
					"           ,[AntecedentItemsCount]\r\n" + 
					"           ,[Consequent]\r\n" + 
					"           ,[Confidence])\r\n" + 
					"     VALUES\r\n" + 
					"           (?\r\n" + 
					"           ,?\r\n" + 
					"           ,?\r\n" + 
					"           ,?\r\n" + 
					"           ,?\r\n" + 
					"           ,?\r\n" + 
					"           ,?\r\n" + 
					"           ,?)", Statement.RETURN_GENERATED_KEYS);
			
			sql.setString(1, "US");
			sql.setString(2,  "US");
			sql.setString(3, "EN");
			sql.setString(4,  "");
			sql.setString(5,  "bsd");
			sql.setString(6,  Integer.toString(rule.javaAntecedent().size()));
			sql.setString(7,  rule.javaConsequent().get(0));
			sql.setString(8, Double.toString(rule.confidence()));
			
			sql.executeUpdate();
			ResultSet rs = sql.getGeneratedKeys();
			int ruleId; 
			if(rs != null && rs.next()) {
				ruleId = rs.getInt(1);
				
				for ( String antecedent : rule.javaAntecedent()) {
					PreparedStatement sqlRuleDetail = connection.prepareStatement("INSERT INTO [dbo].[AssociationRulesDetail]\r\n" + 
							"           ([AssociationRulesMasterId]\r\n" + 
							"           ,[OrderCode])\r\n" + 
							"     VALUES\r\n" + 
							"           (?\r\n" + 
							"           ,?)");
					
					sqlRuleDetail.setInt(1, ruleId );
					sqlRuleDetail.setString(2, antecedent);
					
					sqlRuleDetail.executeUpdate();
				}
				
			}
			connection.commit();
			
		} catch (SQLException e) {
			connection.rollback();
			e.printStackTrace();
		}
	}

	private static ArrayList<String> getTransactions(Connection connection) throws SQLException {
		
		PreparedStatement sql = connection.prepareStatement("select distinct BeaconId, OrderCode from dbo.BasketItem\r\n" + 
				"where BeaconId in (\r\n" + 
				"select BeaconId from [dbo].[BasketItem]\r\n" + 
				"where OrderCode is not null and RTRIM(LTRIM(OrderCode)) <> '' and Country = 'US' and CmsSegment = 'bsd'\r\n" + 
				"group by BeaconId\r\n" + 
				"having count(OrderCode) >= 2)");

		ResultSet result = sql.executeQuery();
		ArrayList<String> orderItems = new ArrayList<String>();
		ArrayList<String> transactions = new ArrayList<String>();
		int tempBeaconId = -1;
		while(result.next()) {
			int beaconId = result.getInt("BeaconId");
			if(beaconId == tempBeaconId || tempBeaconId == -1) {
				tempBeaconId = addOrderItem(result, orderItems, beaconId);
			}else {
				transactions.add(String.join(" ", orderItems));
				orderItems = new ArrayList<String>();
				tempBeaconId = addOrderItem(result, orderItems, beaconId);
			}
		}
		transactions.add(String.join(" ", orderItems));
		return transactions;
	}

	private static int addOrderItem(ResultSet result, ArrayList<String> orderItems, int beaconId) throws SQLException {
		int tempBeaconId;
		orderItems.add(result.getString("OrderCode"));
		tempBeaconId = beaconId;
		return tempBeaconId;
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
