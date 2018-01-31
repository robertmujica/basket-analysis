package com.basketanalysis.repositories;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.basketanalysis.entities.SegmentedBasketItem;

public class BasketItemRepository {

	public ArrayList<SegmentedBasketItem> getSegmentedBasketItems(Connection connection) throws SQLException {

		PreparedStatement sql = connection
				.prepareStatement("select Region, Country, Language, SalesSegment, CmsSegment from dbo.BasketItem (nolock)\r\n" + 
						"where Region is not null and RTRIM(LTRIM(Region)) <> '' and RTRIM(LTRIM(SalesSegment)) <> ''"
						+ "group by Region, Country, Language, SalesSegment, CmsSegment");

		ResultSet result = sql.executeQuery();
		ArrayList<SegmentedBasketItem> items = new ArrayList<SegmentedBasketItem>();
		while(result.next()) {
			SegmentedBasketItem item = new SegmentedBasketItem();
			item.setRegion(result.getString(1));
			item.setCountry(result.getString(2));
			item.setLanguage(result.getString(3));
			item.setSalesSegment(result.getString(4));
			item.setCmsSegment(result.getString(5));
			items.add(item);
		}
		
		return items;
	}

	public ArrayList<String> getTransactions(Connection connection, SegmentedBasketItem segmentedBasketItem) throws SQLException {
		
		PreparedStatement sql = connection.prepareStatement("select distinct BeaconId, OrderCode from dbo.BasketItem\r\n" + 
				"where BeaconId in (\r\n" + 
				"select BeaconId from [dbo].[BasketItem]\r\n" + 
				"where OrderCode is not null and RTRIM(LTRIM(OrderCode)) <> '' and Region = ? and Country = ? and Language = ? and SalesSegment = ? and CmsSegment = ? \r\n" + 
				"group by BeaconId\r\n" + 
				"having count(OrderCode) >= 2)"); 
		
		sql.setString(1,  segmentedBasketItem.getRegion());
		sql.setString(2, segmentedBasketItem.getCountry());
		sql.setString(3,  segmentedBasketItem.getLanguage());
		sql.setString(4, segmentedBasketItem.getSalesSegment());
		sql.setString(5, segmentedBasketItem.getCmsSegment());

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
	
}
