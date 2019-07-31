package utility;

import java.util.Arrays;

public class JCensusData implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	public int age;
	public String workclass;
	public int fnlwgt;
	public String education;
	public int educationNum;
	public String maritalStatus;
	public String occupation;
	public String relationship;
	public String race;
	public String sex;
	public int capitalGain;
	public int capitalLoss;
	public int hoursPerWeek;
	public String nativeCountry;
	public boolean incomeOver50;

	public JCensusData(int age, String workclass, int fnlwgt, String education, int educationNum, String maritalStatus,
			String occupation, String relationship, String race, String sex, int capitalGain, int capitalLoss,
			int hoursPerWeek, String nativeCountry, boolean incomeOver50) {
		this.age = age;
		this.workclass = workclass;
		this.fnlwgt = fnlwgt;
		this.education = education;
		this.educationNum = educationNum;
		this.maritalStatus = maritalStatus;
		this.occupation = occupation;
		this.relationship = relationship;
		this.race = race;
		this.sex = sex;
		this.capitalGain = capitalGain;
		this.capitalLoss = capitalLoss;
		this.hoursPerWeek = hoursPerWeek;
		this.nativeCountry = nativeCountry;
		this.incomeOver50 = incomeOver50;
	}

	public static JCensusData parseLine(String line) {
		String[] p = Arrays.stream(line.split(",")).map((String s) -> s.trim().replaceAll("\"", ""))
				.toArray(String[]::new);
		return new JCensusData(Integer.parseInt(p[0]), p[1], Integer.parseInt(p[2]), p[3], Integer.parseInt(p[4]), p[5],
				p[6], p[7], p[8], p[9], Integer.parseInt(p[10]), Integer.parseInt(p[11]), Integer.parseInt(p[12]),
				p[13], p[14] == ">50K");
	}
}
