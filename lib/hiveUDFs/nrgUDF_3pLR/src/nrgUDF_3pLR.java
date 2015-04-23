/***************************************************************************************************
** Name:		Anomaly Detection Based on Linear Regression Model
** Version:		1.1
** Created by:		Melody Zhao
** Created Date:	09/2014
** Function:		Detect customer extremely high usage based on 3p linear regression model.
**
** Modification:
** Person		Date		Details
** -------              ----            -------
** Melody		03/06/2015	Read in one string including esiid, usage and temperature
**					Take Confidence Level (cl) from the inpur parameter
**
*****************************************************************************************************/

package com.nrg.nrgUDF_3pLR;

import java.lang.Object;
import java.io.IOException;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
//import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import org.apache.mahout.math.*;
import java.util.StringTokenizer;

public final class nrgUDF_3pLR extends UDF {
  public Text evaluate(final Text rec) throws IOException{

	//JavaStringObjectInspector stringInspector;
	//stringInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	//String rec = stringInspector.getPrimitiveJavaObject(input);

    if (rec == null) { return null; }

    String[] fields = rec.toString().split(",");
    String esiid = fields[0];

    int valid_count = Integer.parseInt(fields[1]);
    int base_count = Integer.parseInt(fields[2]);
    
    //convert array of string usages to double usages
    StringTokenizer tokenizer = new StringTokenizer(fields[3], "\\|");
    double[] daily_usage = new double[tokenizer.countTokens()];
    

    int index = 0;
    while (tokenizer.hasMoreTokens()){ daily_usage[index++] = Double.parseDouble(tokenizer.nextToken()); }

    //convert array of string temps to double temps
    tokenizer = new StringTokenizer(fields[4], "\\|");
    double[] daily_temp = new double[tokenizer.countTokens()];
    index = 0;
    while (tokenizer.hasMoreTokens()){ daily_temp[index++] = Double.parseDouble(tokenizer.nextToken()); }

    Vector temp = new DenseVector(daily_temp.length);
    Vector usage = new DenseVector(daily_usage.length);
    usage.assign(daily_usage);
    temp.assign(daily_temp);
    double cl = Double.parseDouble(fields[5]);
    int replication = Integer.parseInt(fields[6]);

    //Call the LinearRegressor
    LinearRegressor BestModel = new LinearRegressor(temp,usage,replication);
    double slope = BestModel.slope(); //r_slp
	double intercept = BestModel.y_intercept(); //Intercept
    double c2 = BestModel.cut2(); //Tcpc
	double baseline = BestModel.baseLine();  //YCP
    double avg_usage = BestModel.avgUsage; // average usage = (min + max)/2
	double max_usage = BestModel.max_usg;
	double min_usage = BestModel.min_usg;
    double rs_DWDS = BestModel.r2_DWDS(temp,usage);
    //double rs_MAPR = BestModel.r2_MAPR(temp, usage);
    double rmse = BestModel.RMSE(temp,usage);
    //double svrc = BestModel.STD(temp,usage);
    //double ci_hw = BestModel.CI_HalfWidth(temp, usage);
    double pi_99 = BestModel.PI_HalfWidth(temp, usage, 0.99);
	double pi_95 = BestModel.PI_HalfWidth(temp, usage, 0.95);
	double pi_90 = BestModel.PI_HalfWidth(temp, usage, 0.9);
	double mean_temp = BestModel.x_mean(temp);
	double ss_x = BestModel.VAR_Reg_Parameter(temp);
	
    //double mean_temp = BestModel.x_mean(temp);
    //double ss_x = BestModel.VAR_Reg_Parameter(temp);



    return new Text(esiid + "|" + String.format("%.8f", slope) + "|" + String.format("%.8f", intercept) + "|" + String.format("%.8f", c2) + "|" + String.format("%.8f", baseline) + "|" + String.format("%.8f", avg_usage) + "|" + String.format("%.8f", max_usage) + "|" + String.format("%.8f", min_usage) + "|" + String.format("%.8f", rs_DWDS) + "|" + String.format("%.8f", rmse) + "|" + String.format("%.8f", pi_99) + "|" + String.format("%.8f", pi_95) + "|" + String.format("%.8f", pi_90) + "|" + String.format("%.8f", mean_temp) + "|" + String.format("%.8f", ss_x) + "|" + valid_count + "|" + base_count);
  }
}