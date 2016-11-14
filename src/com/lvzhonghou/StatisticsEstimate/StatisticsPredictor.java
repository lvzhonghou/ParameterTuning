package com.lvzhonghou.StatisticsEstimate;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.lvzhonghou.Prophet.MapTaskParameters;
import com.lvzhonghou.Prophet.ReduceTaskParameters;
import com.lvzhonghou.common.MapOrReduce;
import com.lvzhonghou.common.MapStatistics;
import com.lvzhonghou.common.ReduceStatistics;
import com.lvzhonghou.common.Statistics;
import com.lvzhonghou.common.StatisticsType;

/**
 * @Description
 * @author zhonghou.lzh
 * @date 2016年7月26日 下午3:04:45
 * @version v1。0
 */

class Interval {
    int position; // -1, 0, 1 represents the left, middle, right
    Statistics statisLeft;
    Statistics statisRight;
}

public class StatisticsPredictor {
    ParaStatisLink paraStatisLink;
    List<Statistic> inactiveStatis;
    List<Statistic> activeStatis;
    List<Parameter> parameters;
    Map<Parameter, ArrayList<Statistic>> paraStatisMap;

    public StatisticsPredictor() {
	paraStatisLink = new ParaStatisLink();
	paraStatisLink.init();

	// init the StatisticsPredictor
	inactiveStatis = paraStatisLink.inactiveStatis;
	activeStatis = paraStatisLink.activeStatis;
	parameters = paraStatisLink.parameters;
	paraStatisMap = paraStatisLink.paraStatisMap;
    }

    public boolean isContain(String str, List<Statistic> statistics) {
	for (Statistic statis : statistics) {
	    if (statis.name.equals(str))
		return true;
	}

	return false;
    }

    public Interval getInterval(List<Statistics> statistics, Parameter para,
	    double paraVal) {
	Interval interval = null;
	if (statistics.size() < 2) {
	    System.out.println("there are not over 2 ");
	    return null;
	}

	interval = new Interval();
	try {
	    if (para.mapOrReduce == MapOrReduce.map
		    || para.mapOrReduce == MapOrReduce.mapandreduce) {
		Class clazz = statistics.get(0).mapParameters.getClass();
		Field field = clazz.getDeclaredField(para.paraName);

		String type = field.getType().toString();
		if (type.endsWith("double") || type.endsWith("Double")) {
		    System.out.println("the type of this parameter is double.");
		} else {
		    System.out
			    .println("the type of this parameter is not effective.");
		    return null;
		}

		int index = 0;
		for (Statistics statis : statistics) {
		    if (field.getDouble(statis.mapParameters) <= paraVal)
			index++;
		    else {
			break;
		    }
		}
		if (index == 0) {
		    interval.position = -1;
		    interval.statisLeft = statistics.get(0);
		    interval.statisRight = statistics.get(1);
		} else if (index >= statistics.size()) {
		    interval.position = 1;
		    interval.statisLeft = statistics.get(statistics.size() - 2);
		    interval.statisRight = statistics
			    .get(statistics.size() - 1);
		} else {
		    interval.position = 0;
		    interval.statisLeft = statistics.get(index - 1);
		    interval.statisRight = statistics.get(index);
		}

	    } else if (para.mapOrReduce == MapOrReduce.reduce) {
		Class clazz = statistics.get(0).reduceParameters.getClass();
		Field field = clazz.getDeclaredField(para.paraName);

		String type = field.getType().toString();
		if (type.endsWith("double") || type.endsWith("Double")) {
		    System.out.println("the type of this parameter is double.");
		} else {
		    System.out
			    .println("the type of this parameter is not effective.");
		    return null;
		}

		int index = 0;
		for (Statistics statis : statistics) {
		    if (field.getDouble(statis.reduceParameters) <= paraVal)
			index++;
		    else
			break;
		}
		if (index == 0) {
		    interval.position = -1;
		    interval.statisLeft = statistics.get(0);
		    interval.statisRight = statistics.get(1);
		} else if (index >= statistics.size()) {
		    interval.position = 1;
		    interval.statisLeft = statistics.get(statistics.size() - 2);
		    interval.statisRight = statistics
			    .get(statistics.size() - 1);
		} else {
		    interval.position = 0;
		    interval.statisLeft = statistics.get(index - 1);
		    interval.statisRight = statistics.get(index);
		}
	    } else {
		System.out.println("there is no parameter type.");
		return null;
	    }
	} catch (NoSuchFieldException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (SecurityException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (IllegalArgumentException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (IllegalAccessException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return interval;
    }

    /*
     * List<Statistics> statisList, all the statistics of the same hostname , so
     * we need to collect the statistics of the same hostname 
     * MapTaskParameters mapParameters, ReduceTaskParameters reduceParameters
     */
    public Statistics statisticsPredict(List<Statistics> statisList,
	    MapTaskParameters mapParameters,
	    ReduceTaskParameters reduceParameters) {
	Statistics statistic = new Statistics();
	statistic.hostName = statisList.get(0).hostName;
	statistic.mapStatistics = new MapStatistics();
	statistic.reduceStatistics = new ReduceStatistics();

	// cal the mapStatistics dataflow and cost which are inactive
	for (Statistic statis : inactiveStatis) {
	    if (statis.mapOrReduce == MapOrReduce.reduce)
		continue;

	    try {
		if (statis.statType == StatisticsType.dataflow) {
		    Class clazz = statistic.mapStatistics.dataFlowStat
			    .getClass();
		    Field field = clazz.getDeclaredField(statis.name);

		    double sumFields = 0;
		    for (Statistics statisticEle : statisList) {
			sumFields += field
				.getDouble(statisticEle.mapStatistics.dataFlowStat);
		    }
		    double newField = sumFields / statisList.size();
		    field.set(statistic.mapStatistics.dataFlowStat, newField);
		} else if (statis.statType == StatisticsType.cost) {
		    Class clazz = statistic.mapStatistics.costStat.getClass();
		    Field field = clazz.getDeclaredField(statis.name);

		    double sumFields = 0;
		    for (Statistics statisticEle : statisList) {
			sumFields += field
				.getDouble(statisticEle.mapStatistics.costStat);
		    }
		    double newField = sumFields / statisList.size();
		    field.set(statistic.mapStatistics.costStat, newField);
		} else {
		    System.out.println("there is no this stattype");
		}

	    } catch (NoSuchFieldException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (SecurityException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (IllegalArgumentException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (IllegalAccessException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }

	}

	// cal the reduceStatistics dataflow and cost which are inactive
	for (Statistic statis : inactiveStatis) {
	    if (statis.mapOrReduce == MapOrReduce.map)
		continue;
	    try {
		if (statis.statType == StatisticsType.dataflow) {
		    Class clazz = statistic.reduceStatistics.reduceDataFlowStatistics
			    .getClass();
		    Field field = clazz.getDeclaredField(statis.name);

		    double sumFields = 0;
		    for (Statistics statisticEle : statisList) {
			sumFields += field
				.getDouble(statisticEle.reduceStatistics.reduceDataFlowStatistics);
		    }
		    double newField = sumFields / statisList.size();
		    field.set(
			    statistic.reduceStatistics.reduceDataFlowStatistics,
			    newField);
		} else if (statis.statType == StatisticsType.cost) {
		    Class clazz = statistic.reduceStatistics.reduceCostStatistics
			    .getClass();
		    Field field = clazz.getDeclaredField(statis.name);

		    double sumFields = 0;
		    for (Statistics statisticEle : statisList) {
			sumFields += field
				.getDouble(statisticEle.reduceStatistics.reduceCostStatistics);
		    }
		    double newField = sumFields / statisList.size();
		    field.set(statistic.reduceStatistics.reduceCostStatistics,
			    newField);
		} else {
		    System.out.println("there is no this stattype");
		}
	    } catch (NoSuchFieldException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (SecurityException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (IllegalArgumentException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (IllegalAccessException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}

	// estimate the active statistics
	for (Parameter parameter : paraStatisMap.keySet()) {
	    List<Statistic> stats = paraStatisMap.get(parameter);
	    final Parameter para = parameter;
	    // sort the statisList
	    Collections.sort(statisList, new Comparator<Statistics>() {

		@Override
		public int compare(Statistics o1, Statistics o2) {
		    try {
			if (para.mapOrReduce == MapOrReduce.map
				|| para.mapOrReduce == MapOrReduce.mapandreduce) {
			    Class clazz = o1.mapParameters.getClass();
			    Field field = clazz.getDeclaredField(para.paraName);
			    String type = field.getType().toString();
			    if (type.endsWith("Double")
				    || type.endsWith("double")) {
				if (field.getDouble(o1.mapParameters) >= field
					.getDouble(o2.mapParameters))
				    return 1;
				else
				    return -1;
			    } else if (type.endsWith("Boolean")
				    || type.endsWith("boolean")) {
				System.out.println("the type of this field is "
					+ "boolean");
				return 0;
			    } else {
				System.out.println("there is no such type");
			    }

			} else if (para.mapOrReduce == MapOrReduce.reduce) {
			    Class clazz = o1.reduceParameters.getClass();
			    Field field = clazz.getDeclaredField(para.paraName);
			    String type = field.getType().toString();
			    if (type.endsWith("Double")
				    || type.endsWith("double")) {
				if (field.getDouble(o1.reduceParameters) >= field
					.getDouble(o2.reduceParameters))
				    return 1;
				else
				    return -1;
			    } else if (type.endsWith("Boolean")
				    || type.endsWith("boolean")) {
				System.out.println("the type of this field is "
					+ "boolean");
				return 0;
			    } else {
				System.out.println("there is no such type");
			    }
			} else {
			    System.out
				    .println("this parameter has no mapOrReduce value");
			}

		    } catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    } catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    } catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    } catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }
		    return 0;
		}
	    });

	    // determine the current parameter belong to which interval
	    double paraval = 0;
	    try {
		if (parameter.mapOrReduce == MapOrReduce.map
			|| parameter.mapOrReduce == MapOrReduce.mapandreduce) {
		    Class clazz = mapParameters.getClass();
		    Field field;

		    field = clazz.getDeclaredField(parameter.paraName);

		    String type = field.getType().toString();

		    if (type.endsWith("double") || type.endsWith("Double")) {
			paraval = field.getDouble(mapParameters);
		    } else {
			System.out
				.println("the type of this parameter is not effective");
			paraval = 0;
		    }
		} else if (parameter.mapOrReduce == MapOrReduce.reduce) {
		    Class clazz = reduceParameters.getClass();
		    Field field = clazz.getDeclaredField(parameter.paraName);
		    String type = field.getType().toString();

		    if (type.endsWith("double") || type.endsWith("Double")) {
			paraval = field.getDouble(reduceParameters);
		    } else {
			System.out
				.println("the type of this parameter is not effective");
			paraval = 0;
		    }
		} else {
		    System.out.println("there is no type");
		}
	    } catch (NoSuchFieldException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (SecurityException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (IllegalArgumentException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (IllegalAccessException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }

	    Interval interval = getInterval(statisList, parameter, paraval);
	    if (interval == null) {
		return statisList.get(0);
	    }
	    try {
		for (Statistic stat : stats) {

		    Statistics statisLeft = interval.statisLeft;
		    Statistics statisRight = interval.statisRight;

		    if (stat.mapOrReduce == MapOrReduce.map
			    && stat.statType == StatisticsType.cost) {
			double[] left = new double[2];
			double[] right = new double[2];

			Class clazz1 = statisLeft.mapParameters.getClass();
			Class clazz2 = statisRight.mapStatistics.costStat
				.getClass();

			Field field1 = clazz1
				.getDeclaredField(parameter.paraName);
			Field field2 = clazz2.getDeclaredField(stat.name);
			left[0] = field1.getDouble(statisLeft.mapParameters); // parameter
									      // value
			right[0] = field1.getDouble(statisRight.mapParameters); // parameter
										// value
			left[1] = field2
				.getDouble(statisLeft.mapStatistics.costStat); // statistics
									       // value
			right[1] = field2
				.getDouble(statisRight.mapStatistics.costStat); // statistics
										// value

			double curPara = field1.getDouble(mapParameters);
			double curStatis = left[1] + (curPara - left[0])
				* ((right[1] - left[1]) / (right[0] - left[0]));
			field2.setDouble(statistic.mapStatistics.costStat, curStatis);

		    } else if (stat.mapOrReduce == MapOrReduce.reduce
			    && stat.statType == StatisticsType.cost) {
			double[] left = new double[2];
			double[] right = new double[2];
			
			Class clazz1 = statisLeft.reduceParameters.getClass();
			Class clazz2 = statisRight.reduceStatistics.reduceCostStatistics.getClass();
			
			Field field1 = clazz1.getDeclaredField(parameter.paraName);
			Field field2 = clazz2.getDeclaredField(stat.name);
			left[0] = field1.getDouble(statisLeft.reduceParameters);
			right[0] = field1.getDouble(statisRight.reduceParameters);
			left[1] = field2.getDouble(statisLeft.reduceStatistics.reduceCostStatistics);
			right[1] = field2.getDouble(statisRight.reduceStatistics.reduceCostStatistics);
			
			double curPara = field1.getDouble(reduceParameters);
			double curStatis = left[1] + (curPara - left[0])
				* ((right[1] - left[1]) / (right[0] - left[0]));
			field2.setDouble(statistic.reduceStatistics.reduceCostStatistics, curStatis);
			
		    } else {
			System.out.println("there is no such type.");
		    }
		}
	    } catch (NoSuchFieldException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (SecurityException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (IllegalArgumentException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (IllegalAccessException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }

	}
	
	return statistic;

    }
}
