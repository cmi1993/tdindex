package cn.edu.scnu.dtindex.dataproc;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import cn.edu.scnu.dtindex.model.Course;
import cn.edu.scnu.dtindex.model.Tuple;
import cn.edu.scnu.dtindex.tools.DFSIOTools;
import cn.edu.scnu.dtindex.tools.IOTools;
import cn.edu.scnu.dtindex.tools.UUIDGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 数据生成器
 */
public class DataGenerate {
	static class NoneOpMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {
		@Override
		protected void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
			super.map(key, value, context);
		}
	}

	private int dataCount = 10000;//元组总数
	private int singlePMCIDCount = 2;//单个PM（Person Message）对应的最大CID数
	private int singleNoTimeCount = 20;//非时态元组重复最大个数
	private int cidMin = 100001;//课程id最小
	private int cidMax = 100100;//课程id最大
	private long vtMax = 0l;//有效时间最小19900101
	private long vtMin = 0l;//有效时间最大

	private ArrayList<Tuple> alTuple = new ArrayList<Tuple>();//时态元组数组
	//private int sid = 0;//非时态元组的序号自增 -->现在改为UUID。

	private final int noTimeCount = 4;//非时态列数

	public DataGenerate() {
		try {
			vtMax = new SimpleDateFormat("YY-MM-DD").parse("2030-04-01").getTime();
			vtMin = new SimpleDateFormat("YY-MM-DD").parse("1980-04-01").getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	public int getNoTimeCount() {
		return noTimeCount;
	}

	public static final String[] COURSE_TITLE={
		"Archaeology and Anthropology", "Biochemistry (Molecular and Cellular)", "Biological Sciences",
				"Biomedical Sciences", "Chemistry", "Classical Archaeology and Ancient History", "Classics",
				"Classics and English", "Classics and Modern Languages", "Classics and Oriental Studies",
				"Computer Science", "Computer Science and Philosophy", "Earth Sciences (Geology)",
				"Economics and Management", "Engineering Science", "English Language and Literature",
				"English and Modern Languages", "European and Middle Eastern Languages", "Fine Art",
				"Geography", "History", "History (Ancient and Modern)", "History and Economics",
				"History and English", "History and Modern", "Languages", "History and Politics",
				"History of Art", "Human Sciences", "Law (Jurisprudence)", "Modern Languages",
				"Modern Languages and Linguistics", "Music", "Oriental Studies", "Philosophy and Modern Languages",
				"Philosophy", " Politics and Economics (PPE)", "Philosophy and Theology", "Physics",
				"Physics and Philosophy", "Psychology (Experimental)", "Psychology", " Philosophy and Linguistics",
				"Religion and Oriental Studies", "Theology and Religion", "Classical Archaeology and Ancient History",
				"Classics", "Classics and Oriental Studies", "History (Ancient and Modern)", "Oriental Studies",
				"Philosophy and Theology", "Religion and Oriental Studies", "Theology and Religion",
				"Economics and Management", "History and Economics", "Philosophy", " Politics and Economics",
				"Modern Languages (only in combination with another language)", "Classics and Modern Languages",
				"English and Modern Languages", "European and Middle Eastern Languages", "History and Modern Languages",
				"Philosophy and Modern Languages", "Classics", "Classics and English", "Classics and Modern Languages",
				"Classics and Oriental Studies", "Classical Archaeology and Ancient History", "History (Ancient and Modern)",
				"Modern Languages (only in combination with another language)", "Classics and Modern Languages",
				"English and Modern Languages", "European and Middle Eastern Languages", "History and Modern Languages",
				"Philosophy and Modern Languages", "Modern Languages", "Classics and Modern Languages",
				"Classics and Oriental Studies", "English and Modern Languages", "European and Middle Eastern Languages",
				"History and Modern Languages", "Oriental Studies", "Philosophy and Modern Languages",
				"Classics and Oriental Studies", "European and Middle Eastern Languages", "Oriental Studies",
				"Religion and Oriental Studies", "Theology and Religion", "Philosophy and Theology",
				"Religion and Oriental Studies", "Archaeology and Anthropology", "Human Sciences", "Philosophy",
				" Politics and Economics", "c++", "java", "hadoop", "hbase", "mapreduce","c"};


	public static final String[] FEMALE_FIRST_NAMES = {
			"Mary", "Patricia", "Linda", "Barbara", "Elizabeth", "Jennifer", "Maria", "Susan",
			"Margaret", "Dorothy", "Lisa", "Nancy", "Karen", "Betty", "Helen", "Sandra", "Donna",
			"Carol", "Ruth", "Sharon", "Michelle", "Laura", "Sarah", "Kimberly", "Deborah", "Jessica",
			"Shirley", "Cynthia", "Angela", "Melissa", "Brenda", "Amy", "Anna", "Rebecca", "Virginia",
			"Kathleen", "Pamela", "Martha", "Debra", "Amanda", "Stephanie", "Carolyn", "Christine",
			"Marie", "Janet", "Catherine", "Frances", "Ann", "Joyce", "Diane", "Alice", "Julie",
			"Heather", "Teresa", "Doris", "Gloria", "Evelyn", "Jean", "Cheryl", "Mildred", "Katherine",
			"Joan", "Ashley", "Judith", "Rose", "Janice", "Kelly", "Nicole", "Judy", "Christina",
			"Kathy", "Theresa", "Beverly", "Denise", "Tammy", "Irene", "Jane", "Lori", "Rachel",
			"Marilyn", "Andrea", "Kathryn", "Louise", "Sara", "Anne", "Jacqueline", "Wanda", "Bonnie",
			"Julia", "Ruby", "Lois", "Tina", "Phyllis", "Norma", "Paula", "Diana", "Annie", "Lillian",
			"Emily", "Robin", "Peggy", "Crystal", "Gladys", "Rita", "Dawn", "Connie", "Florence",
			"Tracy", "Edna", "Tiffany", "Carmen", "Rosa", "Cindy", "Grace", "Wendy", "Victoria", "Edith",
			"Kim", "Sherry", "Sylvia", "Josephine", "Thelma", "Shannon", "Sheila", "Ethel", "Ellen",
			"Elaine", "Marjorie", "Carrie", "Charlotte", "Monica", "Esther", "Pauline", "Emma",
			"Juanita", "Anita", "Rhonda", "Hazel", "Amber", "Eva", "Debbie", "April", "Leslie", "Clara",
			"Lucille", "Jamie", "Joanne", "Eleanor", "Valerie", "Danielle", "Megan", "Alicia", "Suzanne",
			"Michele", "Gail", "Bertha", "Darlene", "Veronica", "Jill", "Erin", "Geraldine", "Lauren",
			"Cathy", "Joann", "Lorraine", "Lynn", "Sally", "Regina", "Erica", "Beatrice", "Dolores",
			"Bernice", "Audrey", "Yvonne", "Annette", "June", "Samantha", "Marion", "Dana", "Stacy",
			"Ana", "Renee", "Ida", "Vivian", "Roberta", "Holly", "Brittany", "Melanie", "Loretta",
			"Yolanda", "Jeanette", "Laurie", "Katie", "Kristen", "Vanessa", "Alma", "Sue", "Elsie",
			"Beth", "Jeanne"};
	public static final String[] MALE_FIRST_NAMES = {
			"James", "John", "Robert", "Michael", "William", "David", "Richard", "Charles", "Joseph",
			"Thomas", "Christopher", "Daniel", "Paul", "Mark", "Donald", "George", "Kenneth", "Steven",
			"Edward", "Brian", "Ronald", "Anthony", "Kevin", "Jason", "Matthew", "Gary", "Timothy",
			"Jose", "Larry", "Jeffrey", "Frank", "Scott", "Eric", "Stephen", "Andrew", "Raymond",
			"Gregory", "Joshua", "Jerry", "Dennis", "Walter", "Patrick", "Peter", "Harold", "Douglas",
			"Henry", "Carl", "Arthur", "Ryan", "Roger", "Joe", "Juan", "Jack", "Albert", "Jonathan",
			"Justin", "Terry", "Gerald", "Keith", "Samuel", "Willie", "Ralph", "Lawrence", "Nicholas",
			"Roy", "Benjamin", "Bruce", "Brandon", "Adam", "Harry", "Fred", "Wayne", "Billy", "Steve",
			"Louis", "Jeremy", "Aaron", "Randy", "Howard", "Eugene", "Carlos", "Russell", "Bobby",
			"Victor", "Martin", "Ernest", "Phillip", "Todd", "Jesse", "Craig", "Alan", "Shawn",
			"Clarence", "Sean", "Philip", "Chris", "Johnny", "Earl", "Jimmy", "Antonio", "Danny",
			"Bryan", "Tony", "Luis", "Mike", "Stanley", "Leonard", "Nathan", "Dale", "Manuel", "Rodney",
			"Curtis", "Norman", "Allen", "Marvin", "Vincent", "Glenn", "Jeffery", "Travis", "Jeff",
			"Chad", "Jacob", "Lee", "Melvin", "Alfred", "Kyle", "Francis", "Bradley", "Jesus", "Herbert",
			"Frederick", "Ray", "Joel", "Edwin", "Don", "Eddie", "Ricky", "Troy", "Randall", "Barry",
			"Alexander", "Bernard", "Mario", "Leroy", "Francisco", "Marcus", "Micheal", "Theodore",
			"Clifford", "Miguel", "Oscar", "Jay", "Jim", "Tom", "Calvin", "Alex", "Jon", "Ronnie",
			"Bill", "Lloyd", "Tommy", "Leon", "Derek", "Warren", "Darrell", "Jerome", "Floyd", "Leo",
			"Alvin", "Tim", "Wesley", "Gordon", "Dean", "Greg", "Jorge", "Dustin", "Pedro", "Derrick",
			"Dan", "Lewis", "Zachary", "Corey", "Herman", "Maurice", "Vernon", "Roberto", "Clyde",
			"Glen", "Hector", "Shane", "Ricardo", "Sam", "Rick", "Lester", "Brent", "Ramon", "Charlie",
			"Tyler", "Gilbert", "Gene"};
	public static final String[] LAST_NAMES = {
			"Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore",
			"Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia",
			"Martinez", "Robinson", "Clark", "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen",
			"Young", "Hernandez", "King", "Wright", "Lopez", "Hill", "Scott", "Green", "Adams", "Baker",
			"Gonzalez", "Nelson", "Carter", "Mitchell", "Perez", "Roberts", "Turner", "Phillips",
			"Campbell", "Parker", "Evans", "Edwards", "Collins", "Stewart", "Sanchez", "Morris",
			"Rogers", "Reed", "Cook", "Morgan", "Bell", "Murphy", "Bailey", "Rivera", "Cooper",
			"Richardson", "Cox", "Howard", "Ward", "Torres", "Peterson", "Gray", "Ramirez", "James",
			"Watson", "Brooks", "Kelly", "Sanders", "Price", "Bennett", "Wood", "Barnes", "Ross",
			"Henderson", "Coleman", "Jenkins", "Perry", "Powell", "Long", "Patterson", "Hughes",
			"Flores", "Washington", "Butler", "Simmons", "Foster", "Gonzales", "Bryant", "Alexander",
			"Russell", "Griffin", "Diaz", "Hayes", "Myers", "Ford", "Hamilton", "Graham", "Sullivan",
			"Wallace", "Woods", "Cole", "West", "Jordan", "Owens", "Reynolds", "Fisher", "Ellis",
			"Harrison", "Gibson", "Mcdonald", "Cruz", "Marshall", "Ortiz", "Gomez", "Murray", "Freeman",
			"Wells", "Webb", "Simpson", "Stevens", "Tucker", "Porter", "Hunter", "Hicks", "Crawford",
			"Henry", "Boyd", "Mason", "Morales", "Kennedy", "Warren", "Dixon", "Ramos", "Reyes", "Burns",
			"Gordon", "Shaw", "Holmes", "Rice", "Robertson", "Hunt", "Black", "Daniels", "Palmer",
			"Mills", "Nichols", "Grant", "Knight", "Ferguson", "Rose", "Stone", "Hawkins", "Dunn",
			"Perkins", "Hudson", "Spencer", "Gardner", "Stephens", "Payne", "Pierce", "Berry",
			"Matthews", "Arnold", "Wagner", "Willis", "Ray", "Watkins", "Olson", "Carroll", "Duncan",
			"Snyder", "Hart", "Cunningham", "Bradley", "Lane", "Andrews", "Ruiz", "Harper", "Fox",
			"Riley", "Armstrong", "Carpenter", "Weaver", "Greene", "Lawrence", "Elliott", "Chavez",
			"Sims", "Austin", "Peters", "Kelley", "Franklin", "Lawson"};
	public final static String[] EMAIL_SUFFIX = {"qq.com", "126.com", "163.com", "gmail.com",
			"163.net", "msn.com", "hotmail.com", "yahoo.com.cn", "sina.com", "@mail.com", "263.net", "sohu.com",
			"21cn.com", "sogou.com"
	};

	private final static <T> T nextValue(T[] array) {
		assert (array != null && array.length > 0);
		return array[new Random().nextInt(array.length)];
	}

	/**
	 * -----------------
	 * 产生随机英文名
	 * -----------------
	 *
	 * @return 随机的英文名称
	 */
	public final static String getRandomEnglishName() {
		return getRandomEnglishFirstName() + " " + getRandomEnglishLastName();
	}

	//随机FirstName
	private final static String getRandomEnglishFirstName() {
		return new Random().nextBoolean() ? nextValue(FEMALE_FIRST_NAMES) : nextValue(MALE_FIRST_NAMES);
	}

	//随机LastName
	private final static String getRandomEnglishLastName() {
		return nextValue(LAST_NAMES);
	}

	/**
	 * -----------------
	 * 产生随机的邮箱
	 * -----------------
	 *
	 * @return 随机的邮箱字符串
	 */
	public final static String getRandomEmailAddress() {
		return getRandomEnglishFirstName() + getRandomEnglishLastName() + "@" + nextValue(EMAIL_SUFFIX);
	}




	/**
	 * ---------------
	 * 产生多个课程id
	 * ---------------
	 *
	 * @param count 个数
	 * @return 课程id列表
	 */
	private ArrayList<Object> getCouresID(int count) {
		ArrayList<Object> obj = new ArrayList<Object>();
		for (int i = 0; i < count; i++) {
			obj.add(getCourseID());
		}
		return obj;
	}

	/**
	 * 产生课程id
	 *
	 * @return int类型的课程id
	 */
	private int getCourseID() {
		int id = (int) (cidMin + Math.random() * (cidMax - cidMin));
		return id;
	}

	/**
	 * 产生有效时间组
	 *
	 * @param count 数量
	 * @return 有效时间对列表
	 */
	private ArrayList<long[]> getValueTime(int count) {
		ArrayList<long[]> alValueTime = new ArrayList<long[]>();//有效时间对数组
		for (int i = 0; i < count; i++) {
			long[] tempValueTime = randomValueTime();
			alValueTime.add(tempValueTime);
		}
		return alValueTime;
	}


	//根据最大值，最小值随机产生一对有效时间
	private long[] randomValueTime() {
		long[] vt = new long[2];
		vt[0] = (long) (vtMin + Math.random() * (vtMax - vtMin));//有效时间开始
		vt[1] = (long) (vt[0] + Math.random() * (vtMax - vt[0]));//有效时间结束
		return vt;
	}

	/**
	 * ---------------
	 * 产生个人信息
	 * ---------------
	 *
	 * @return 个人信息Object数组
	 */
	private Object[] getRandomPersonMessage() {
		Object[] obj = new Object[noTimeCount - 1];
		//obj[0] = sid++;//id
		obj[0] = UUIDGenerator.getUUID();
		obj[1] = getRandomEnglishName();//名称
		obj[2] = getRandomEmailAddress();//地址
		return obj;
	}

	//产生一个非时态元组
	public Object[] randomNoTime() {
		Object[] obj = new Object[noTimeCount];
		Object[] objPM = getRandomPersonMessage();
		for (int i = 0; i < noTimeCount - 1; i++) {
			obj[i] = objPM[i];
		}
		obj[noTimeCount - 1] = getCourseID();
		return obj;
	}


	public ArrayList<Course> getAllCourse(int dataCount){
		ArrayList<Course> allCourse = new ArrayList<Course>(dataCount);
		int l =0;
		for (int i = 0; i < dataCount; i++) {
			Object[] objPM = getRandomPersonMessage();
			int randomCidCount = (int) (1 + (Math.random() * singlePMCIDCount));//个人信息的Cid个数

			//2.个人信息+CID
			ArrayList<Object> alCid = getCouresID(randomCidCount);//产生randomCidCount个CouresID
			for (int j = 0; j < alCid.size(); j++) {
				Integer cid = (Integer)(alCid.get(j));
				String teacher_name = (String) objPM[1];
				ArrayList<long[]> valueTime = getValueTime(1);
				long start_time = valueTime.get(0)[0];
				long end_time = valueTime.get(0)[1];
				Course course = new Course(cid,teacher_name,start_time,end_time);
				allCourse.add(course);
				l++;
				if (l>=dataCount)break;
			}
			if (l>=dataCount)
				break;

		}
		return allCourse;
	}

	//产生所有的元组
	public ArrayList<Tuple> getAllTuple() {
		int l = 0;
		for (int i = 0; i < dataCount; i++) {
			//1.个人信息
			Object[] objPM = getRandomPersonMessage();
			int randomCidCount = (int) (1 + (Math.random() * singlePMCIDCount));//个人信息的Cid个数
			//2.个人信息+CID
			ArrayList<Object> alCid = getCouresID(randomCidCount);//产生randomCidCount个CouresID
			for (int j = 0; j < alCid.size(); j++) {//个人信息和Cid组合
				Object[] ntObj = new Object[noTimeCount];
				for (int iPM = 0; iPM < noTimeCount - 1; iPM++) {
					ntObj[iPM] = objPM[iPM];
				}
				ntObj[noTimeCount - 1] = alCid.get(j);

				int randomVtCount = (int) (1 + (Math.random() * singleNoTimeCount));//单非时态元组对应的有效时间个数
				//2.个人信息+CID+有效时间
				ArrayList<long[]> alVt = getValueTime(randomVtCount);//产生重复非时态元组的多个有效时间
//				System.out.println(randomVtCount);
				for (int k = 0; k < alVt.size(); k++) {
					if (l >= dataCount) break;//达到dataCount时，结束
					Tuple tuple = new Tuple(ntObj, alVt.get(k));
					alTuple.add(tuple);
//					System.out.println(alTuple.get(l));
					l++;
				}
				if (l >= dataCount) break;//达到dataCount时，结束
			}
			if (l >= dataCount) break;//达到dataCount时，结束
		}
		return alTuple;
	}

	public int getDataCount() {
		return dataCount;
	}

	public void setDataCount(int dataCount) {
		this.dataCount = dataCount;
	}

	/**
	 * 产生10w条数据
	 *
	 * @param path--路径
	 * @throws IOException
	 */
	public static void Generate50w(String path) throws IOException {
		DataGenerate pd = new DataGenerate();
		ArrayList<Tuple> allTuple;
		allTuple = new ArrayList<Tuple>();
		pd.setDataCount(500000);
		allTuple = pd.getAllTuple();
		StringBuilder str = new StringBuilder();
		for (Tuple t : allTuple) {
			str.append(t.toString());
			str.append("\n");
		}
		DFSIOTools.toWrite(new Configuration(), str.toString(), path, 1);
		str = null;
		allTuple.clear();


	}


	public static void GenerateCourseTable(int count,String dfsPath) throws IOException {
		DataGenerate pd = new DataGenerate();
		ArrayList<Course> allCourse = pd.getAllCourse(count);
		StringBuilder str = new StringBuilder();
		for (Course c : allCourse) {
		    str.append(c.toString()).append("\n");
		}

		DFSIOTools.toWrite(new Configuration(), str.toString(), dfsPath, 1);
		str = null;
		allCourse.clear();
	}

	public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

		startCourseJob();
	}

	private static String startCourseJob() throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();
		job.setJobName("data generate");
		job.setMapperClass(NoneOpMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		/***************************
		 *
		 *......
		 *在这里，和普通的MapReduce一样，设置各种需要的东西
		 *......
		 ***************************/

		//下面为了远程提交添加设置：
		Configuration conf = job.getConfiguration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("fs.default", "hdfs://master:8020");
		conf.set("mapred.child.java.opts", "-Xmx1024m");
		conf.set("mapreduce.job.jar", "/home/think/idea project/dtindex/target/dtindex-1.0-SNAPSHOT-jar-with-dependencies.jar");
		GenerateCourseTable(10000,"/timeData/1000w/course.txt");

		FileInputFormat.setInputPaths(job, "/null");
		Path outPath = new Path("/generateData_info/");//用于mr输出success信息的路径
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
		job.submit();
		//提交以后，可以拿到JobID。根据这个JobID可以打开网页查看执行进度。
		return job.getJobID().toString();

	}

	public static String startStudentJob() throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();
		job.setJobName("data generate");
		job.setMapperClass(NoneOpMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		/***************************
		 *
		 *......
		 *在这里，和普通的MapReduce一样，设置各种需要的东西
		 *......
		 ***************************/

		//下面为了远程提交添加设置：
		Configuration conf = job.getConfiguration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("fs.default", "hdfs://master:8020");
		conf.set("mapred.child.java.opts", "-Xmx1024m");
		conf.set("mapreduce.job.jar", "/Users/think/tdindex/target/dtindex-1.0-SNAPSHOT-jar-with-dependencies.jar");
		for (int i = 0; i < 100; i++) {
			Generate50w("/timeData/5000w/data.txt");
			System.out.println((i + 1) * 50 + "w");
		}

		FileInputFormat.setInputPaths(job, "/null");
		Path outPath = new Path("/generateData_info/");//用于mr输出success信息的路径
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
		job.submit();
		//提交以后，可以拿到JobID。根据这个JobID可以打开网页查看执行进度。
		return job.getJobID().toString();


	}
}

