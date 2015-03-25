import java.util.ArrayList;
import java.util.List;

public class Study {

	private String c3dStudyIdentifier;
	private String btrisStudyIdentifier;
	private String studyStatus;
	List<StudySubject> studySubjects;
	
	public Study(){
		studySubjects=new ArrayList<StudySubject>();
	}

	public List<StudySubject> getStudySubjects() {
		return studySubjects;
		}

	public void setStudySubjects(List<StudySubject> studySubjects) {
		this.studySubjects = studySubjects;
		}
	
	public String getC3DStudyIdentifier() {
		return c3dStudyIdentifier;
	}
	public String getBTRISStudyIdentifier() {
		return btrisStudyIdentifier;
	}
	public String getStudyStatus() {
		return studyStatus;
	}
	public void setC3DStudyIdentifier(String studyIdentifier) {
		this.c3dStudyIdentifier = studyIdentifier;
	}
	public void setBTRISStudyIdentifier(String studyIdentifier) {
		this.btrisStudyIdentifier = studyIdentifier;
	}
	public void setStudyStatus(String studyStatus) {
		this.studyStatus = studyStatus;
	}

}
