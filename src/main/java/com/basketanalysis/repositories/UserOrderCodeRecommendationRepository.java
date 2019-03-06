package com.basketanalysis.repositories;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.spark.mllib.fpm.AssociationRules.Rule;

import com.basketanalysis.entities.SegmentedBasketItem;

public class UserOrderCodeRecommendationRepository {

	public void deleteRecommendations(Connection connection, SegmentedBasketItem segmentedBasketItem) throws SQLException {
		
		try {
			PreparedStatement detailSql = connection.prepareStatement("delete from [dbo].[UserOrderCodeRecommendation]\r\n" + 
					"  where Region = ? and Country = ? and Language = ? and SalesSegment = ? and CmsSegment = ? and CatalogId = ?");
			
			detailSql.setString(1,  segmentedBasketItem.getRegion());
			detailSql.setString(2, segmentedBasketItem.getCountry());
			detailSql.setString(3,  segmentedBasketItem.getLanguage());
			detailSql.setString(4, segmentedBasketItem.getSalesSegment());
			detailSql.setString(5,  segmentedBasketItem.getCmsSegment());
			detailSql.setInt(6,  segmentedBasketItem.getCatalogId());
			detailSql.executeUpdate();
			
			connection.commit();
		} catch (SQLException e) {
			connection.rollback();
			e.printStackTrace();
		}
	}

	public void insertRecommendation(int userId, int orderCodeId, double rating, Connection connection, SegmentedBasketItem segmentedBasketItem) throws SQLException {
		PreparedStatement sql;
		try {
			sql = connection.prepareStatement("INSERT INTO [dbo].[UserOrderCodeRecommendation]\r\n" + 
					"           ([Region]\r\n" + 
					"           ,[Country]\r\n" + 
					"           ,[Language]\r\n" + 
					"           ,[SalesSegment]\r\n" + 
					"           ,[CmsSegment]\r\n" + 
					"           ,[CatalogId]\r\n" + 
					"           ,[UserId]\r\n" + 
					"           ,[OrderCodeId]\r\n" + 
					"           ,[Rating])\r\n" + 
					"     VALUES\r\n" + 
					"           (?\r\n" + 
					"           ,?\r\n" + 
					"           ,?\r\n" + 
					"           ,?\r\n" + 
					"           ,?\r\n" + 
					"           ,?\r\n" + 
					"           ,?\r\n" + 
					"           ,?\r\n" + 
					"           ,?)", Statement.RETURN_GENERATED_KEYS);
			
			sql.setString(1, segmentedBasketItem.getRegion());
			sql.setString(2,  segmentedBasketItem.getCountry());
			sql.setString(3, segmentedBasketItem.getLanguage());
			sql.setString(4,  segmentedBasketItem.getSalesSegment());
			sql.setString(5,  segmentedBasketItem.getCmsSegment());
			sql.setInt(6,  segmentedBasketItem.getCatalogId());
			sql.setInt(7,  userId);
			sql.setInt(8,  orderCodeId);
			sql.setDouble(9, rating);
			
			sql.executeUpdate();
			
			connection.commit();
			
		} catch (SQLException e) {
			connection.rollback();
			e.printStackTrace();
		}
	}
}
