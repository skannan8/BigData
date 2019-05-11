import java.util.*;
public class ClarinetScheduler implements Scheduler{
	double overallExecutionTime = 0;
    List<String> querySchedule = null;
    double minAccuracy;

    public double runScheduler(List<QEPObject> input, double memory)
    {
    	overallExecutionTime = 0;
    	querySchedule = new ArrayList<String>();
    	List<QEPObject> inputSchedule = new ArrayList<>(input);
        //Sort in SJF order 
		Collections.sort(inputSchedule, new Comparator<QEPObject>() {

			@Override
			public int compare(QEPObject o1, QEPObject o2) {
				double diff = o1.time - o2.time;
				return diff > 0 ? (int) Math.ceil(diff) : (int) Math.floor(diff);
			}

		});
        
        int i=0;
        double currentTime = 0;
        double currentMaxTime= 0;
        PriorityQueue<QEPObject> currentRunning =
                new PriorityQueue<QEPObject>(new PQComparator());
        boolean cantSchedule = false;
        
        // Now as shortest task finishes, free up memory and add possible remaining tasks
        // within memory bound.
        while(inputSchedule.size() > 0)
        {
            if (currentRunning.size() > 0){
                double minTime = currentRunning.peek().getEndTime();

                // remove all completed task with the same end time
                while (currentRunning.size() > 0 &&
                        currentRunning.peek().getEndTime() == minTime)
                {
                    QEPObject completedTask = currentRunning.remove();
                    currentTime = completedTask.getEndTime();
                    memory += completedTask.getMemory();
                    cantSchedule = false;
                }
                overallExecutionTime = Math.max(overallExecutionTime, currentTime);
            }
            if(cantSchedule) {
            	break;
            }
            i=0;
            while (inputSchedule.size() > 0)
            {
                while(i < inputSchedule.size() &&
                    memory - inputSchedule.get(i).getMemory() < 0)
                {
                    i++;
                }
                if (i >= inputSchedule.size()) {
                	cantSchedule = true;
                	break;
                }

                // add to output schedule
                querySchedule.add((inputSchedule.get(i).name) + "\n");

                // decrement available memory
                memory -= inputSchedule.get(i).getMemory();

                // set start, end time for the task
                inputSchedule.get(i).setStartTime(currentTime);
                inputSchedule.get(i).setEndTime(currentTime + inputSchedule.get(i).getTime());

                // add to current running set
                currentRunning.add(new QEPObject(inputSchedule.get(i)));
                currentMaxTime= Math.max(currentMaxTime,
                        inputSchedule.get(i).getEndTime());

                // remove from query list to be run
                inputSchedule.remove(i);
            }
        }
        overallExecutionTime = Math.max(overallExecutionTime, currentMaxTime);
        double returnValue = inputSchedule.size() > 0 ? -1d : overallExecutionTime;
       
        return returnValue;
    }
}
