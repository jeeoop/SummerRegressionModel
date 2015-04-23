/***************************************************************************************************
** Name:		3P Linear Regression Model
** Version:		1.1
** Created by:		Melody Zhao
** Created Date:	09/2015
** Function:		3p linear regression model based on summer (Jun - Sep) hourly usage data.
**
** Modification:
** Person		Date		Details
** -------          	----        	-------
** Melody Zhao		03/06/2015	
**
** result: effective_dt = '2014-10-01'
*****************************************************************************************************/
package com.nrg.nrgUDF_3pLR;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.*;
import org.apache.mahout.math.function.DoubleDoubleFunction;
import org.apache.mahout.math.function.DoubleFunction;
import org.apache.mahout.math.function.Functions;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Simple segmented linear regressor.  This uses a simplification of the method of Muggeo.  This is
 * based on using two dummy variables per change-point to fit a three part linear model where the
 * center segment has zero slope.
 * <p/>
 * The basic idea is that we want to solve a linear system that looks like this:
 * <p/>
 * [y = begin{cases}
 * mathrm{if \,\,} x < c1 & a0 x + b0 \\
 * mathrm{if \,\,} x < c2 & b1 \\
 * mathrm{else\,\,} a2 x + b2 } \]
 */

public class LinearRegressor {
    private static final int MAX_ITERATIONS = 200;

    private static final int GAMMA = 0;
    private static final int BETA = 1;
    //private static final int GAMMA2 = 2;
    //private static final int BETA2 = 3;
    private static final int INTERCEPT = 2;

    public boolean converged;
	public double avgUsage;
	public double max_usg;
	public double min_usg;
    //private double c1;
    private double c2;
    private Vector parms = new DenseVector(3);

    public LinearRegressor(Vector x, Vector y) {
        this(x, y, 0);
    }

    /**
    *This constructor is intended to support external fitting methods.
    *
    *@param cutoff1    Cutoff between left slope and flat (middle) segment
	*@param cutoff2    Cutoff between left slope and flat (middle) segment
	*@param ycp        Y value for the middle (flat) segment
	*@param leftSlope  Slope of the left-most segment
	*@param rightSlope Slope of the right-most segment
	**/
    public LinearRegressor(double cutoff2, double ycp, double rightSlope) {
        //c1 = cutoff1;
        c2 = cutoff2;
        parms.set(INTERCEPT, ycp);
        //parms.set(BETA1, leftSlope);
        parms.set(BETA, rightSlope);
        //parms.set(GAMMA1, - leftSlope * cutoff1);
        parms.set(GAMMA, - rightSlope * cutoff2);
    }

    public LinearRegressor(Vector x, Vector yValues, int restarts) {
        Preconditions.checkArgument(x.size() == yValues.size());
        Preconditions.checkArgument(x.size() > 2, "Not enough input to produce a model");

        Random rand = RandomUtils.getRandom();

        Matrix y = new DenseMatrix(yValues.size(), 1).assignColumn(0, yValues);

        List<Double> xDistribution = Lists.newArrayList(Iterables.transform(x.all(), new Function<Vector.Element, Double>() {
            @Override
            public Double apply(Vector.Element input) {
                return input.get();
            }
        }));

        Collections.sort(xDistribution);
        double min = xDistribution.get(0);
        double max = xDistribution.get(x.size() - 1);
		
		//double min_temp = x.minValue();
		//double max_temp = x.maxValue();
		
		List<Double> yDistribution = Lists.newArrayList(Iterables.transform(yValues.all(), new Function<Vector.Element, Double>() {
            @Override
            public Double apply(Vector.Element input) {
                return input.get();
            }
        }));
		
		Collections.sort(yDistribution);
        min_usg = yDistribution.get(0);
        max_usg = yDistribution.get(yValues.size() - 1);
		
		//double minUsage = yValues.minValue();
		//double maxUsage = yValues.minValue();
		avgUsage = (min_usg + max_usg) * 0.5;

        int n = Math.max(1, restarts);
        Vector bestModel = new DenseVector(5);
        double bestResidual = Double.MAX_VALUE;

	for (int start = 0; start < n; start++) {
            if (restarts > 0) {
                // pick starting points at random
                double t0 = xDistribution.get(rand.nextInt(x.size()));
                double t1 = xDistribution.get(rand.nextInt(x.size()));
                c2 = Math.min(t0, t1);
                //c2 = Math.max(t0, t1);
		} else {
                //c1 = (2 * min + max) / 3;
                c2 = (3 * min + max) / 4;
            }
            Vector model = fitData(min, max, x, y, c2);

            double residual = yValues.minus(predict(model.viewPart(1, 3), x)).norm(1);
            if (!Double.isNaN(residual) && residual < bestResidual) {
                bestModel = model;
                bestResidual = residual;
            }
        }

        extractModel(bestModel);

	// tune up final result to force continuity
	tuneUp(x, yValues, true);


    }

    public void tuneUp(Vector x, Vector y, boolean tuneBaseline) {
        Vector mask = new DenseVector(x.size());

	// tune up baseline
		mask.assign(x).assign(new DoubleFunction() {
            @Override
            public double apply(double x) {
                return (x <= c2) ? 1 : 0;
            }
        });
        //boolean adjustLeft = false;
        boolean adjustRight = false;
        if (tuneBaseline) {
            if (mask.zSum() > 0) {
                parms.set(INTERCEPT, y.dot(mask) / mask.zSum());

			// if we had a valid baseline, we can adjust left and right sides to connect to baseline
				//adjustLeft = true;
                adjustRight = true;
            } else {
			// with no points involved in baseline, we simply accept value at c2
                double newBaseline = parms.get(GAMMA) + parms.get(BETA) * c2;
                parms.set(INTERCEPT, newBaseline);
                parms.set(GAMMA, parms.get(GAMMA) - newBaseline);
                //adjustLeft = true;
            }
        }
        else {
            //adjustLeft = true;
            adjustRight = true;
        }
/*
        if (adjustLeft) {
            mask.assign(x).assign(new DoubleFunction() {
                @Override
                public double apply(double x) {
                    return x < c1 ? 1 : 0;
                }
            });
            if (mask.zSum() > 0) {
                parms.viewPart(GAMMA1, 2).assign(solveHinge(x, y, mask, c1, baseLine()));
                parms.set(GAMMA1, parms.get(GAMMA1) - baseLine());
            } else {
                parms.set(GAMMA1, 0);
                parms.set(BETA1, 0);
            }
        }
*/
        if (adjustRight) {
		// and right side the same way
		mask.assign(x).assign(new DoubleFunction() {
                @Override
                public double apply(double x) {
                    return x > c2 ? 1 : 0;
                }
            });
            if (mask.zSum() > 0) {
                parms.viewPart(GAMMA, 2).assign(solveHinge(x, y, mask, c2, baseLine()));
                parms.set(GAMMA, parms.get(GAMMA) - baseLine());
            } else {
                parms.set(GAMMA, 0);
                parms.set(BETA, 0);
            }
        }
    }

    private Vector fitData(double min, double max, Vector x, Matrix y, double c2) {
        Vector parms = null;
        //converged = true; //boolean converged = true;

        //double oldC1 = c1;
        double oldC2 = c2;
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            Matrix m = setupDummyArgs(c2, x);
            parms = solve(m, y);
            //double deltaC1 = (parms.get(BETA1) * c1 + parms.get(GAMMA1)) / parms.get(BETA1) * 0.5;
            double deltaC2 = (parms.get(BETA) * c2 + parms.get(GAMMA)) / parms.get(BETA) * 0.5;

            if (Double.isInfinite(deltaC2) || Double.isNaN(deltaC2)) {
                c2 = (min + 2 * c2) / 3;
            } else {
                double newValue = c2 - deltaC2;
                if (newValue > max) {
                    c2 = max;
                } else if (newValue < c2) {
                    c2 = (min + c2) / 2;
                } else {
					c2 = newValue;//c2 = (newValue+c2)/2;
				}
            }

            if (Math.abs(oldC2 - c2) < 1e-3) {
                converged = true;
                return modelVector(c2, parms);
            }
            //oldC1 = c1;
            oldC2 = c2;
        }
        converged = false;
        return modelVector(c2, parms);
    }

    private Vector modelVector(double c2, Vector parameters) {
        Vector r = new DenseVector(4);
        r.set(0, c2);
        r.viewPart(1, 3).assign(parameters);
        return r;
    }

    private void extractModel(Vector model) {
        c2 = model.get(0);
        parms.assign(model.viewPart(1, 3));
    }

    public Vector predict(Vector x) {
        return predict(c2, parms, x);
    }
	
	public Vector predict2(Vector x) {
        return predict(parms, x);
    }

    public Vector predict(double c2, Vector parms, Vector x) {
        Matrix m = setupDummyArgs(c2, x);
        return m.times(parms);
    }
	
	public Vector predict(Vector parms, Vector x) {
        Matrix m = setupDummyArgs2(x);
        return m.times(parms);
    }

	/**
	*Least squares solution of a + b x = y, subject to constraint that a + b x_0 = y_0 exactly.
	*
	*@param xBase X values to fit
	*@param yBase Y values to fit
	*@param mask  Which values to pay attention to
	*@param x0    X value for constraint
	*@param y0    Y value for constraint
	*@return Intercept and slope for the resulting constrained least squares solution.
	**/

	public static Vector solveHinge(Vector xBase, Vector yBase, Vector mask, double x0, double y0) {
        Vector x = xBase.clone().plus(-x0).assign(mask, Functions.MULT);
        Vector y = yBase.clone().plus(-y0).assign(mask, Functions.MULT);

        double slope = x.dot(y) / x.dot(x);
        double offset = y0 - slope * x0;
        return new DenseVector(new double[]{offset, slope});
    }

    public double predict(double x) {
        return predict(new DenseVector(new double[]{x})).get(0);
    }

    public double cut2() {
        return c2;
    }

    public double slope() {
        return parms.get(BETA);
    }

    public Vector ab2() {
        return parms.viewPart(Math.min(BETA, GAMMA), 2);
    }
	
	public double max_usage(){
		return max_usg;
	}
	
	public double min_usage(){
		return min_usg;
	}
	
    public double baseLine() {
        return parms.get(INTERCEPT);
    }

	public double y_intercept() {
        return parms.get(GAMMA) + parms.get(INTERCEPT);
    }
	
    public double RMSE(Vector x, Vector y) {
        //return Math.sqrt(predict2(x).minus(y).aggregate(Functions.PLUS, Functions.SQUARE) / x.size());
		return Math.sqrt(predict2(x).minus(y).aggregate(Functions.PLUS, Functions.SQUARE) / (x.size() - 2));
    }
    
    public double r2_MAPR(Vector x, Vector y) {
        double SS_data = y.plus(-y.zSum() / y.size()).aggregate(Functions.PLUS, Functions.SQUARE);
        Vector errors = predict2(x).minus(y);
        double SS_error = errors.plus(-errors.zSum() / y.size()).aggregate(Functions.PLUS, Functions.SQUARE);
        return 1 - SS_error / SS_data;
    }
    
    public double r2_DWDS(Vector x, Vector y) {
        double SS_total = y.plus(-y.zSum() / y.size()).aggregate(Functions.PLUS, Functions.SQUARE); 
        double SS_res = predict2(x).minus(y).aggregate(Functions.PLUS, Functions.SQUARE);
        return 1 - SS_res / SS_total;
    }
	
	public double STD(Vector x, Vector y){
		return Math.sqrt(predict2(x).minus(y).aggregate(Functions.PLUS, Functions.SQUARE) / (x.size()-1));
	}
	
	public double VAR_Reg_Parameter(Vector x){
		double v_agg_square = x.aggregate(Functions.PLUS, Functions.SQUARE);
		double v_square_agg = Math.pow(x.zSum(), 2);
		return v_agg_square - v_square_agg/x.size();
	}
	
	public double PI_HalfWidth(Vector x, Vector y, double cl){
		double rmse = RMSE(x,y);
		double pi_hw = 0;
		if (cl == 0.9)
			pi_hw = 1.645*rmse;
		if (cl == 0.95)
			pi_hw = 1.96*rmse;
		if (cl == 0.99)
			pi_hw = 2.576*rmse;
		return pi_hw;
	}
	
	public double CI_HalfWidth(Vector x, Vector y, double cl){
		double rmse = RMSE(x,y);
		double ci_hw = 0;
		if (cl == 0.9)
			ci_hw = 1.645*rmse/Math.sqrt(x.size());
		if (cl == 0.95)
			ci_hw = 1.96*rmse/Math.sqrt(x.size());
		if (cl == 0.99)
			ci_hw = 2.576*rmse/Math.sqrt(x.size());	
		return ci_hw;
	}
	
	public double x_mean (Vector x){
		return x.zSum()/x.size();
	}

    public Vector getParameters() {
        return parms;
    }
    
    private Vector solve(Matrix m, Matrix y) {
        Matrix z = new QRDecomposition(m).solve(y);
        return z.viewColumn(0);
    }

    private Matrix setupDummyArgs(final double c2, Vector x1) {
        Matrix m = new DenseMatrix(x1.size(), 3);
        m.viewColumn(BETA).assign(x1, new DoubleDoubleFunction() {
            @Override
            public double apply(double zero, double x) {
                return x > c2 ? x : 0;
            }
        });
        m.viewColumn(GAMMA).assign(x1, new DoubleDoubleFunction() {
            @Override
            public double apply(double zero, double x) {
                return x > c2 ? 1 : 0;
            }
        });
        m.viewColumn(INTERCEPT).assign(1);
        return m;
    }
	
    private Matrix setupDummyArgs2(Vector x1) {
        Matrix m = new DenseMatrix(x1.size(), 3);
        m.viewColumn(BETA).assign(x1);
        m.viewColumn(GAMMA).assign(1);
        m.viewColumn(INTERCEPT).assign(1);
        return m;
    }


}
