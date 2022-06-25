本次实验的两个任务我们打包成了一个JAR包提交，通过指定主类的方式来完成对应任务。JAR包执行方式如下：
	任务1: hadoop jar lab3/lab3.jar UniversityCountryJoiner input_path1 output_dir1
	任务2: hadoop jar lab3/lab3.jar CourseUniversityJoiner input_path2 output_dir2
其中input_path是输入文件所在路径，output_path是输出目录，该目录要求在运行前不存在，由程序自动创建。