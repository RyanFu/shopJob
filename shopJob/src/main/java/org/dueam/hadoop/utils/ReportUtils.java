package org.dueam.hadoop.utils;


public abstract class ReportUtils {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(charReport(new String[][]{{"排名","价格","属性"},{"假ads","2","774"},{"112","33222","dd"}}));

	}

	public static StringBuffer charReport(String[][] values){
		int[] lens = new int[values.length];
		for(int i=0;i<values[0].length;i++){
			lens[i] = 0;
			for(int k=0;k<values.length;k++){
				if(lens[i] < values[k][i].getBytes().length){
					lens[i] = values[k][i].getBytes().length;
				}
			}
		}
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<values.length;i++){
			sb.append('│');
			for (int j = 0; j < values[i].length; j++) {
				sb.append(' ');
				sb.append(values[i][j]);
				for(int k=values[i][j].getBytes().length ;k<lens[j]+1;k++){
					sb.append(' ');
				}
				sb.append(' ');
				sb.append('│');
			}
			sb.append('\n');
		}

		return sb;
	}

}
