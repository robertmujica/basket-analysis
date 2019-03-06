package com.basketanalysis.poc;

import java.lang.reflect.Array;
import scala.Tuple2;

import org.apache.spark.api.java.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.AssociationRules.Rule;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.basketanalysis.domain.BasketAnalysisService;
import com.basketanalysis.domain.CollaborativeFilteringService;
import com.basketanalysis.entities.SegmentedBasketItem;
import com.basketanalysis.repositories.AssociationRulesRepository;
import com.basketanalysis.repositories.BasketItemRepository;
import com.google.common.collect.Sets;

import scala.Console;

public class App 
{
	public static void main( String[] args ) throws Exception
	{
		//BasketAnalysisService basketAnalysisService = new BasketAnalysisService();
		//basketAnalysisService.calculateAssociationRules();
		
		CollaborativeFilteringService service = new CollaborativeFilteringService();
		service.computeRecommendations();
		
		//sparkSample();
		//collaborativeFiltering(args);
	}
	
	/// Ralloy good sample http://www.stepbystepcoder.com/building-a-recommendation-engine-with-apache-spark-java-part-1/
	private static void collaborativeFiltering(String[] args) {
		
		class ParseRating implements Function<String, Rating> {
		    private final Pattern COMMA = Pattern.compile(",");

		    public Rating call(String line) {
		      String[] tok = COMMA.split(line);
		      int x = Integer.parseInt(tok[0]);
		      int y = Integer.parseInt(tok[1]);
		      double rating = Double.parseDouble(tok[2]);
		      return new Rating(x, y, rating);
		    }
		  }

		  class FeaturesToString implements Function<Tuple2<Object, double[]>, String> {
		    public String call(Tuple2<Object, double[]> element) {
		      return element._1() + "," + Arrays.toString(element._2());
		    }
		  }
		  
		  /*if (args.length < 4) {
		      System.err.println(
		        "Usage: JavaALS <ratings_file> <rank> <iterations> <output_dir> [<blocks>]");
		      System.exit(1);
		    }*/
		    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("FP-growth Example");
		    
		    int rank = 10; //Integer.parseInt(args[1]);
		    int iterations = 10; //Integer.parseInt(args[2]);
		    String outputDir = "output"; //args[3];
		    int blocks = -1;
		    if (args.length == 5) {
		      blocks = Integer.parseInt(args[4]);
		    }
		
		    JavaSparkContext sc = new JavaSparkContext(sparkConf);
		    JavaRDD<String> lines = sc.textFile("data/collaborativeFilteringData.txt"); //sc.textFile(args[0]);
		    
		    JavaRDD<Rating> ratings = lines.map(new ParseRating());
		    
		    //Step 1 ) Split rating into training and test
		    JavaRDD<Rating>[] ratingSplits = ratings.randomSplit(new double[] { 0.8, 0.2 });
		    
	        JavaRDD<Rating> trainingRatingRDD = ratingSplits[0].cache();
	        JavaRDD<Rating> testRatingRDD = ratingSplits[1].cache();
	 
	        long numOfTrainingRating = trainingRatingRDD.count();
	        long numOfTestingRating = testRatingRDD.count();
	 
	        System.out.println("Number of training Rating : " + numOfTrainingRating);
	        System.out.println("Number of training Testing : " + numOfTestingRating);
	        
	        //Step 2 ) Create prediction model (Using ALS)
	        ALS als = new ALS();
	        MatrixFactorizationModel model = als.setRank(10).setIterations(10).run(trainingRatingRDD);
	        
	        //Step 3 ) Get the top 5 movie predictions for user
	        Rating[] recommendedsFor4169 = model.recommendProducts(2, 5);
	        System.out.println("Recommendations for 4169");
	        for (Rating rating : recommendedsFor4169) {
	            System.out.println("user : " + rating.user() + " Product id : " + rating.product() + "-- Rating : " + rating.rating());
	        }
		    
		    System.out.println("Final user/product features written to " + outputDir);

		    sc.stop();
	}

	private static void sparkSample() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaALS");
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
