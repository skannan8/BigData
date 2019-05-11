import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class State{
	 List<QEPObject> schedule = new ArrayList<>();
	 double score = -1d;
	 double makeSpan = Double.MAX_VALUE;
	 double accuracy = 0d;
	
	public State(List<QEPObject> schedule) {
		this.schedule = schedule;
		this.accuracy = getAvgAccuracy();
	}
	public State(List<QEPObject> schedule, int score) {
		this.schedule = schedule;
		this.score = score;
	}
			
	//calc average accuracy
	public double getAvgAccuracy() {
		double accuracy = 0;
		for(QEPObject qep : schedule) {
			accuracy += qep.getAccuracy();
			//System.out.println(qep.getAccuracy());
		}
		return accuracy/schedule.size();
	}
}


public class HillClimbing {
	private double memoryLimit;
	private double timeBound = 0d;
	private double accBound = 0d;
	private Scheduler sched;	
	private Random rand;
	private List<List<QEPObject>> visitedSchedules; //This list hold the list of schedules
	private List<List<QEPObject>> inputList; // This holds list of queries and plans for each
	State currState;
	int totalCombinations;
	int problemType; // Either accuracy bound or time bound
	static final int ACCURACY_BOUND = 1;
	static final int TIME_BOUND = 2;
	
	public HillClimbing(double memoryLimit, double bound, int problemType) {
		this.memoryLimit = memoryLimit;	
		//sched = new GreedyScheduler();
		sched = new BestFitPacking();
		rand = new Random(100);
		visitedSchedules = new ArrayList<>();
		this.problemType = problemType;
		if(problemType == ACCURACY_BOUND) {
			accBound = bound;
		}else {
			timeBound = bound;
		}
	}

	public void setInputList(List<List<QEPObject>> input) {
		this.inputList = input;
	
	}

	//method to calculate the score/heuristic of the state
	private double calculateScore(State state) {
		state.makeSpan = sched.runScheduler(state.schedule, memoryLimit);
		if(problemType == ACCURACY_BOUND) {
			if(state.accuracy < accBound || state.makeSpan == -1d) {
				return Double.MAX_VALUE;
			}
			return state.makeSpan;
		}else {
			if(state.makeSpan > timeBound || state.makeSpan == -1d) {
				return -1d;
			}
			return state.accuracy;
		}
		
	}
	
	private boolean hasBeenVisited(State state) {
		for(List<QEPObject> schedule : visitedSchedules) {
			if(schedule.equals(state.schedule)) {
				return true;
			}
		}
		return false;
	}
	
	//generate successor state
	private State getNextState() {
		State newState = null;
		List<QEPObject> schedule = new ArrayList<>();
		boolean visited = true;
		while(visited && !(visitedSchedules.size() > totalCombinations)) {
			schedule.clear();
			for(List<QEPObject> obj : inputList) {
				schedule.add(obj.get(rand.nextInt(obj.size())));
			}
			newState = new State(schedule);
			visited = hasBeenVisited(newState);
		}
		if(newState != null) {		
			newState.score = calculateScore(newState);
		}
		
		//System.out.println(newState.score);
		visitedSchedules.add(schedule);
		return newState;
	}
	
	public List<QEPObject> getOptimalSchedule() {
		totalCombinations =  10000;//(int) Math.pow(inputList.get(0).size(), inputList.size());

		currState = getNextState();
		if(currState == null) {
			System.out.println("Not enough resources and bounds to schedule");
			return null;
		}
		while(currState.score == -1d) {
			currState = getNextState();
			if(currState == null) {
				System.out.println("Not enough resources and bounds to schedule");
				return null;
			}
		}
	    //Just running the algorithm for some number of iterations. Need to discuss on how to determine this
		//Right now I have just fixed it to 3/4th of the possible combos
		boolean exit = false;
		while(!(visitedSchedules.size() > totalCombinations)){
			State newState = getNextState();
			if(newState == null) {
				break;
			}
			while(newState.score == -1d && !(visitedSchedules.size() > totalCombinations)) {
				newState = getNextState();
				if(newState == null) {
					exit = true;
					break;
				}
			}
			if(exit) {
				break;
			}
			if(problemType == ACCURACY_BOUND) {
				if(newState.score < currState.score) {
					currState = newState;
				}else if(Math.abs(newState.score - currState.score) < 0.0001) {
					currState = newState.accuracy > currState.accuracy ? newState : currState;
				}
			}else {
				if(newState.score > currState.score) {
					currState = newState;
				}else if(Math.abs(newState.score - currState.score) < 0.0001) {
					currState = newState.makeSpan < currState.makeSpan ? newState : currState;
				}
			}
			
		}
		System.out.println("total combo : "+ totalCombinations + " visited schedules: " + visitedSchedules.size());
		System.out.println("Average accuracy : " + currState.accuracy);		
		System.out.println("Makespan : " + currState.makeSpan);
		return currState.schedule;
	}

}
