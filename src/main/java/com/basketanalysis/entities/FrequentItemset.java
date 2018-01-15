package com.basketanalysis.entities;

public class FrequentItemset {

	private int _hashCode;
	private long _frequency;
	
	public FrequentItemset(int hashCode, long frequency) {
		_hashCode = hashCode;
		_frequency = frequency;
	}
	
	public int getHashCode() {
		return _hashCode;
	}
	
	public long getFrequency() {
		return _frequency;
	}
	
}
