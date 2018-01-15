package com.basketanalysis.repositories;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.spark.mllib.fpm.AssociationRules.Rule;

import com.basketanalysis.entities.SegmentedBasketItem;

public class AssociationRulesRepository {

	public void deleteRules(Connection connection, SegmentedBasketItem segmentedBasketItem) throws SQLException {
		
		try {
			// remove detail records
			PreparedStatement detailSql = connection.prepareStatement("delete [dbo].[AssociationRulesDetail]\r\n" + 
					"  where AssociationRulesMasterId in (\r\n" + 
					"  select Id from dbo.AssociationRulesMaster\r\n" + 
					"  where Region = ? and Country = ? and Language = ? and SalesSegment = ? and CmsSegment = ?)");
			
			detailSql.setString(1,  segmentedBasketItem.getRegion());
			detailSql.setString(2, segmentedBasketItem.getCountry());
			detailSql.setString(3,  segmentedBasketItem.getLanguage());
			detailSql.setString(4, segmentedBasketItem.getSalesSegment());
			detailSql.setString(5,  segmentedBasketItem.getCmsSegment());
			detailSql.executeUpdate();
			
			// remove master records
			PreparedStatement masterSql = connection.prepareStatement("DELETE FROM [dbo].[AssociationRulesMaster]\r\n" + 
					"      where Region = ? and Country = ? and Language = ? and SalesSegment = ? and CmsSegment = ?");
			
			masterSql.setString(1,  segmentedBasketItem.getRegion());
			masterSql.setString(2, segmentedBasketItem.getCountry());
			masterSql.setString(3,  segmentedBasketItem.getLanguage());
			masterSql.setString(4, segmentedBasketItem.getSalesSegment());
			masterSql.setString(5,  segmentedBasketItem.getCmsSegment());
			masterSql.executeUpdate();
			
			connection.commit();
		} catch (SQLException e) {
			connection.rollback();
			e.printStackTrace();
		}
	}

	public void insertRules(Rule<String> rule, Connection connection, SegmentedBasketItem segmentedBasketItem, Long itemsetFrequency) throws SQLException {
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
					"           ,[Confidence], "
					+ 			"[ItemsetFrequency])\r\n" + 
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
			sql.setString(6,  Integer.toString(rule.javaAntecedent().size()));
			sql.setString(7,  rule.javaConsequent().get(0));
			sql.setDouble(8, rule.confidence());
			sql.setLong(9, itemsetFrequency);
			
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
	
}
