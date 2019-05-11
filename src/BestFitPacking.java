import java.util.*;

public class BestFitPacking implements Scheduler{
	//parallel arrays to track each level's state
	List<List<QEPObject>> levels;
	List<Double> remainingMemory;
	
	public double runScheduler(List<QEPObject> input, double memory)
    {
		double overallExecutionTime = 0;
		levels = new ArrayList<>();
		remainingMemory = new ArrayList<>();
		List<QEPObject> inputSchedule = new ArrayList<>(input);
		
        //Sort in descending order of time
        Collections.sort(inputSchedule);
        
        //Add initial level
        levels.add(new ArrayList<QEPObject>());
        remainingMemory.add(memory);
        
        for(int i=0; i < inputSchedule.size(); i++) {
        	QEPObject qp = inputSchedule.get(i);
        	if(qp.getMemory() > memory) {
        		return -1d;
        	}
        	
        	double minResidue = memory;
        	int minLevel = -1;
        	for(int levelNum=0; levelNum < levels.size(); levelNum++) { 
        		double residue = remainingMemory.get(levelNum) - qp.getMemory();
        		if(residue < 0) {
            		continue;
            	}
        		if(Double.compare(residue, minResidue) < 0) {
        			minResidue = residue;
        			minLevel = levelNum;
        		}
        	}
        	if(minLevel == -1) { // Then the qp doesn't fit in any of the available levels
        		//Create a new level
        		levels.add(new ArrayList<QEPObject>());
                remainingMemory.add(memory);
                minLevel = levels.size()-1; 
                minResidue = memory - qp.getMemory();
        	}
        	//Add the qp to the chosen level and update remaining memory
        	levels.get(minLevel).add(qp);
        	remainingMemory.set(minLevel, minResidue);
        }
        
        //Since we have sorted in descending order of time, the first qp in each level is the max of
        // times  in the level
        for(int levelNum=0; levelNum < levels.size(); levelNum++) { 
        	overallExecutionTime += levels.get(levelNum).get(0).time;
    	}
        //System.out.println(overallExecutionTime);
		return overallExecutionTime;
    }

}
