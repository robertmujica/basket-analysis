package com.basketanalysis.entities;

public class SegmentedBasketItem {

	private String region;
	private String country;
	private String language;
	private String salesSegment;
	private String cmsSegment;
	private int catalogId;
	
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	public String getSalesSegment() {
		return salesSegment;
	}
	public void setSalesSegment(String salesSegment) {
		this.salesSegment = salesSegment;
	}
	public String getCmsSegment() {
		return cmsSegment;
	}
	public void setCmsSegment(String cmsSegment) {
		this.cmsSegment = cmsSegment;
	}
	public int getCatalogId() {
		return catalogId;
	}
	public void setCatalogId(int catalogId) {
		this.catalogId = catalogId;
	}
	
}
