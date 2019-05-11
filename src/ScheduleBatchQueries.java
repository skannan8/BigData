import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Scanner;
import java.util.List;

public class ScheduleBatchQueries {
	static int numOfQueries = 10; //default
	static int plansPerQuery = 2; //default
	
	public static List<List<QEPObject>> getInputData()
    {
		List<List<QEPObject>> result = new ArrayList<>();
        for (int i = 0; i< numOfQueries; i++)
        {
        	List<QEPObject> list = new ArrayList<>();
            for (int j = 0; j< plansPerQuery; j++)
            {
                list.add(new QEPObject(
                        "Plan #" + Integer.toString(i) + ",  "+  Integer.toString(j),
                        70 + j*10,
                        70 + j*10 + i*10,
                        70 + j*10 + i*10));
               // System.out.println("Plan #" + Integer.toString(i) + ",  "+  Integer.toString(j));
            }
            result.add(list);
        }
        return result;
    }
    public static List<List<QEPObject>> getInputData(String path) throws Exception
    {
    	 Scanner sc = new Scanner(new File(path));
       
    	List<List<QEPObject>> result = new ArrayList<>();
        for (int i = 0; i< numOfQueries; i++)
        {
        	if(!sc.hasNextLine()) {
        		break;
        	}
        	
        	String[] oneQuery = sc.nextLine().trim().split(",");
        	List<QEPObject> list = new ArrayList<>();
            for (int j = 0; j< oneQuery.length; j+=2)
            {
            	
            	double mem = oneQuery[j] == null || oneQuery[j].isEmpty() ? 0d : Double.parseDouble(oneQuery[j]);
            	double time = oneQuery[j+1] == null || oneQuery[j+1].isEmpty() ? 0d : Double.parseDouble(oneQuery[j+1]);
            	if(mem == 0d || time == 0d) {
            		continue;
            	}
            	
            	list.add(new QEPObject(
                        "Plan #" + Integer.toString(i+1) +  Integer.toString(j/2+1),
                        80 + ((j/2)*5),
                        mem,
                        time));
                //System.out.println(list.get(list.size()-1).name+ "\t" + list.get(list.size()-1).accuracy + " "+ list.get(list.size()-1).memory + " "+ list.get(list.size()-1).time);
            }
            //System.out.println();
            result.add(list);
        }
        sc.close();
        return result;
    }
	
	public static void main(String[] args) {
		final double accuracy = 85.0;
        final double memory = 10;
        double timeBound = 40;
        int[][] data = new int[][]{{4,1},{4,3},{5,2},{5,9},{5,5},{14,2},{4,4},{5,5},{4,4},{5,5}};
        List<List<QEPObject>> inputList = null;
        
        System.out.println("Input plans");
        try {
            inputList = getInputData("input.csv");

        }catch(Exception ex) {
        	System.out.println(ex.getMessage());
        }
//        HillClimbing hc = new HillClimbing(memory, accuracy, 1);
//		hc.setInputList(inputList);
//		System.out.println("output schedule");
//		List<QEPObject> schedule = hc.getOptimalSchedule();
//		
//		if(schedule == null) {
//			return;
//		}
//		for(QEPObject qp : schedule) {
//			System.out.println(qp.name);
//		}	
        
        //Get the list sorted by shortest time first
        List<QEPObject> list = new ArrayList<>();
        double resultAcc = 0d;
        for(List<QEPObject> obj : inputList) {
        	 Collections.sort(obj);
        	 list.add(obj.get(obj.size()-1));
        	 resultAcc +=  obj.get(obj.size()-1).accuracy;
		}
        ClarinetScheduler cs = new ClarinetScheduler();

        double time = cs.runScheduler(list, memory);
        System.out.println("Accuracy : " + resultAcc/list.size() + " Makespan: "+ time);
		
//      List<QEPObject> list = new ArrayList<>();
//      double resultAcc = 0d;
//      for(List<QEPObject> obj : inputList) {
//    	  Collections.sort(obj, new Comparator<QEPObject>() {
//
//  			@Override
//  			public int compare(QEPObject o1, QEPObject o2) {
//  				double diff = o2.accuracy - o1.accuracy;
//  				return diff > 0 ? (int) Math.ceil(diff) : (int) Math.floor(diff);
//  			}
//          	
//          });
//      	 list.add(obj.get(0));
//      	 resultAcc +=  obj.get(0).accuracy;
//		}
//      ClarinetScheduler cs = new ClarinetScheduler();
//      //GreedyScheduler gs = new GreedyScheduler();
//      double time = cs.runScheduler(list, memory);
//      System.out.println("Accuracy : " + resultAcc/list.size() + " Makespan: "+ time);
			
	}
}
