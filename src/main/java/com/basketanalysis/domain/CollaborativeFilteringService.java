package com.basketanalysis.domain;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.basketanalysis.entities.FrequentItemsetsResponse;
import com.basketanalysis.entities.SegmentedBasketItem;
import com.basketanalysis.repositories.BasketItemRepository;
import com.basketanalysis.repositories.UserOrderCodeRatingRepository;
import com.basketanalysis.repositories.UserOrderCodeRecommendationRepository;

import scala.Tuple2;

public class CollaborativeFilteringService {

	public static void computeRecommendations() throws SQLException {
		
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
		
		Connection connection = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;databaseName=BasketAnalysis","sa", "Password1");
		connection.setAutoCommit(false);
		
		UserOrderCodeRatingRepository repository = new UserOrderCodeRatingRepository();
		UserOrderCodeRecommendationRepository userRecommendations = new UserOrderCodeRecommendationRepository();
		
		ArrayList<SegmentedBasketItem> segmentedRatings = repository.getSegmentedRatings(connection);
		
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("FP-growth Example");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		int totalRules = 0;
		int rank = 10; //Integer.parseInt(args[1]);
	    int iterations = 10; //Integer.parseInt(args[2]);
	    String outputDir = "output"; //args[3];
	    int blocks = -1;
		for ( SegmentedBasketItem segmentedRating : segmentedRatings) {
			
			
			ArrayList<String> dbTransactions = repository.getUserOrderCodeRatings(connection, segmentedRating);
			
			System.out.println("****************  Generating recommendations for: " + segmentedRating.getRegion() + "- " + segmentedRating.getCountry() + " - " +  
					segmentedRating.getLanguage() + " - " 
					+ segmentedRating.getSalesSegment() + " - " + segmentedRating.getCmsSegment() + " - Catalog: " + segmentedRating.getCatalogId() + 
					" Total transactions :" + dbTransactions.size() + "   ********************");
			
			JavaRDD<String> rddTransactions = sc.parallelize(dbTransactions);
			
			JavaRDD<Rating> ratings = rddTransactions.map(new ParseRating());
			
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
	        MatrixFactorizationModel model = als.setRank(10).setIterations(10).run(ratings);
	        
	        //Step 3 ) Get the top 5 ordercode predictions for user
	        ArrayList<Integer> userList = repository.getSegmentedUsers(connection, segmentedRating);
	        
	        userRecommendations.deleteRecommendations(connection, segmentedRating);
	        for ( int userId :  userList) {
	        	Rating[] recommendations = model.recommendProducts(userId, 5);
	        	for (Rating rating : recommendations) {
		            System.out.println("user : " + rating.user() + " Product id : " + rating.product() + "-- Rating : " + rating.rating());
		            
		            userRecommendations.insertRecommendation(rating.user(), rating.product(), rating.rating(), connection, segmentedRating);
		            
		        }
	        }
		}
		connection.close();
		sc.stop();
		sc.close();

		//JavaRDD<List<String>> basketTransactions = rddTransactions.map(line -> Arrays.asList(line.split(" ")));

		//FrequentItemsetsResponse freqItemsetResponse = generateFrequentItemsets(dbTransactions, basketTransactions);
		
		//totalRules += generateAssociationRules(connection, segmentedBasketItem, freqItemsetResponse);
		System.out.println("Colloborative filtering completed ... ");
		
		    //JavaRDD<String> lines = sc.textFile("data/collaborativeFilteringData.txt"); //sc.textFile(args[0]);
		    
		    //JavaRDD<Rating> ratings = lines.map(new ParseRating());
		    
		    
	
	}
}
