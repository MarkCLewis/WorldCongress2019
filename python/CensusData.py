class CensusData:
    def __init__(self, age, workclass, fnlwgt, education, educationNum, maritalStatus, occupation, relationship, \
        race, sex, capitalGain, capitalLoss, hoursPerWeek, nativeCountry, incomeOver50):
        self.age = age
        self.workclass = workclass
        self.fnlwgt = fnlwgt
        self.education = education
        self.educationNum = educationNum
        self.maritalStatus = maritalStatus
        self.occupation = occupation
        self.relationship = relationship
        self.race = race
        self.sex = sex
        self.capitalGain = capitalGain
        self.capitalLoss = capitalLoss
        self.hoursPerWeek = hoursPerWeek
        self.nativeCountry = nativeCountry
        self.incomeOver50 = incomeOver50

def parseLine(line):
    p = list(map(lambda s: s.strip().replace("\"", ""), line.split(",")))
    return CensusData(int(p[0]), p[1], int(p[2]), p[3], int(p[4]), p[5], \
        p[6], p[7], p[8], p[9], int(p[10]), int(p[11]), int(p[12]), p[13], \
        p[14] == ">50K")
