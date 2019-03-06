package com.basketanalysis.repositories;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.basketanalysis.entities.SegmentedBasketItem;

public class UserOrderCodeRatingRepository {

	public ArrayList<SegmentedBasketItem> getSegmentedRatings(Connection connection) throws SQLException {

		PreparedStatement sql = connection
				.prepareStatement("select Region, Country, Language, SalesSegment, CmsSegment, CatalogId from dbo.userordercoderating (nolock)\r\n" + 
						"where Region is not null and RTRIM(LTRIM(Region)) <> '' and RTRIM(LTRIM(SalesSegment)) <> ''"
						+ "group by Region, Country, Language, SalesSegment, CmsSegment, CatalogId");

		ResultSet result = sql.executeQuery();
		ArrayList<SegmentedBasketItem> items = new ArrayList<SegmentedBasketItem>();
		while(result.next()) {
			SegmentedBasketItem item = new SegmentedBasketItem();
			item.setRegion(result.getString(1));
			item.setCountry(result.getString(2));
			item.setLanguage(result.getString(3));
			item.setSalesSegment(result.getString(4));
			item.setCmsSegment(result.getString(5));
			item.setCatalogId(Integer.parseInt(result.getString(6)));
			items.add(item);
		}
		
		return items;
	}
	
	public ArrayList<Integer> getSegmentedUsers(Connection connection, SegmentedBasketItem segmentedBasketItem) throws SQLException {

		PreparedStatement sql = connection
				.prepareStatement("select UserId from dbo.userordercoderating (nolock)\r\n" + 
						"where Region = ? and Country = ? and Language = ? and SalesSegment = ? and CmsSegment = ? and CatalogId = ?\r\n"
						+ " group by UserId");

		sql.setString(1,  segmentedBasketItem.getRegion());
		sql.setString(2, segmentedBasketItem.getCountry());
		sql.setString(3,  segmentedBasketItem.getLanguage());
		sql.setString(4, segmentedBasketItem.getSalesSegment());
		sql.setString(5, segmentedBasketItem.getCmsSegment());
		sql.setInt(6, segmentedBasketItem.getCatalogId());
		
		ResultSet result = sql.executeQuery();
		ArrayList<Integer> items = new ArrayList<Integer>();
		while(result.next()) {
			int userId = result.getInt("UserId");
			items.add(userId);
		}
		
		return items;
	}
	
public ArrayList<String> getUserOrderCodeRatings(Connection connection, SegmentedBasketItem segmentedBasketItem) throws SQLException {
		
		PreparedStatement sql = connection.prepareStatement("select UserId, OrderCodeId, Rating from dbo.UserOrderCodeRating\r\n" + 
				"where Region = ? and Country = ? and Language = ? and SalesSegment = ? and CmsSegment = ? and CatalogId = ?"); 
		
		sql.setString(1,  segmentedBasketItem.getRegion());
		sql.setString(2, segmentedBasketItem.getCountry());
		sql.setString(3,  segmentedBasketItem.getLanguage());
		sql.setString(4, segmentedBasketItem.getSalesSegment());
		sql.setString(5, segmentedBasketItem.getCmsSegment());
		sql.setInt(6, segmentedBasketItem.getCatalogId());

		ResultSet result = sql.executeQuery();
		ArrayList<String> transactions = new ArrayList<String>();

		while(result.next()) {
			int userId = result.getInt("UserId");
			int orderCodeId = result.getInt("OrderCodeId");
			float rating = result.getFloat("Rating");
			transactions.add(userId + "," + orderCodeId + "," + rating);
		}
		return transactions;
	}
	
}
