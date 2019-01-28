package sub1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * 날짜 : 2019/01/28
 * 개발 : 김민규
 * 내용 : 하둡 파일 입출력 테스트
 */

public class HadoopIOTest {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path(args[0]);
		
		// HDFS 파일 쓰기
		FSDataOutputStream out = hdfs.create(path);
		out.writeUTF(args[1]);
		
		out.close();
		
		// HDFS 파일 읽기
		FSDataInputStream in = hdfs.open(path);
		String txt = in.readUTF();
		in.close();
		
		System.out.println("내용출력 : " + txt);
		
	}
}
