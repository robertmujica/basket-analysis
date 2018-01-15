package com.basketanalysis.entities;

import java.util.Hashtable;

import org.apache.spark.mllib.fpm.FPGrowthModel;

public class FrequentItemsetsResponse {

	private Hashtable<Integer, Long> _frequentItemsets;
	private FPGrowthModel<String> _model;
	
	public FrequentItemsetsResponse(Hashtable<Integer, Long> frequentItemsets, FPGrowthModel<String> model) {
		_frequentItemsets = frequentItemsets;
		_model = model;
	}

	public Hashtable<Integer, Long> getFrequentItemsets() {
		return _frequentItemsets;
	}

	public FPGrowthModel<String> getModel() {
		return _model;
	}
}
