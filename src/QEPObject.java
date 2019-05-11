
public class QEPObject implements Comparable<QEPObject> {
	String name;
	double accuracy;
	double time;
	double memory;
	double startTime = 0;
	double endTime = Integer.MAX_VALUE;

	QEPObject(String name, double accuracy, double memory, double time) {
		super();
		this.name = name;
		this.accuracy = accuracy;
		this.time = time;
		this.memory = memory;
	}

	QEPObject(String name, double accuracy, double time, double memory, double startTime, double endTime) {
		this.name = name;
		this.accuracy = accuracy;
		this.time = time;
		this.memory = memory;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	QEPObject(QEPObject qepObject) {
		this(qepObject.name, qepObject.accuracy, qepObject.time, qepObject.memory, qepObject.startTime,
				qepObject.endTime);
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setAccuracy(double accuracy) {
		this.accuracy = accuracy;
	}

	public double getAccuracy() {
		return accuracy;
	}

	public void setTime(double time) {
		this.time = time;
	}

	public double getTime() {
		return time;
	}

	public void setMemory(double memory) {
		this.memory = memory;
	}

	public double getMemory() {
		return memory;
	}

	public void setStartTime(double startTime) {
		this.startTime = startTime;
	}

	public double getStartTime() {
		return startTime;
	}

	public void setEndTime(double endTime) {
		this.endTime = endTime;
	}

	public double getEndTime() {
		return endTime;
	}

	@Override
	public int compareTo(QEPObject other) {
		if (this.time < other.time)
			return 1;
		else if (this.time == other.time) {
			if (this.memory < other.memory)
				return 1;
			else if (this.memory > other.memory)
				return -1;
			return 0;
		} else
			return -1;
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof QEPObject))
			return false;
		if (other == this)
			return true;
		QEPObject obj = (QEPObject) other;
		return this.accuracy == obj.accuracy && this.name.equals(obj.name) && this.memory == obj.memory
				&& this.time == obj.time;
	}

	@Override
	public int hashCode() {
		return this.name.hashCode();
	}
}
