package home.spark.sql;

import java.io.Serializable;
import java.sql.Date;

public class Person implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private String fullName;
	private String sex;
	private int age;
	private Long dob;

	public Person(String fullName, String sex, int age, Long dob) {
		this.fullName = fullName;
		this.sex = sex;
		this.age = age;
		this.dob = dob;
	}

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public Long getDob() {
		return dob;
	}

	public void setDob(Long dob) {
		this.dob = dob;
	}
}
