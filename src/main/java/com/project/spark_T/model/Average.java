package com.project.spark_T.model;

import java.io.Serializable;

public class Average implements Serializable{

private int count;

private double sum;

public Average(int count, double sum) {
	
				super();
				this.count = count;
				this.sum = sum;
				}
				
				public int getCount() {
				return count;
				
				}
				
				public void setCount(int count) {
				this.count = count;
				
				}
				
				public double getSum() {
				return sum;
				
				}
				
				public void setSum(double sum) {
				this.sum = sum;
				
				}
				
				public double average() {
				return getSum() / getCount();
				
				}

}