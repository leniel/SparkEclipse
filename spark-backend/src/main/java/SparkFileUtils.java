
/**
 * @author Leniel Macaferi
 *
 * 12-2016
 */

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import java.io.FileInputStream;

/**
 * Spark produces multiple files. It's a feature and it is produced by how your RDD is partitioned,
 * hence it is separated in N parts where N is the number of partitions.
 * To "fix" this, one needs to change the number of partitions to 1, by using repartition on the RDD.
 * Coalesce is the recommended option to reduce the number of partitions. However, reducing the number of
 * partitions to one is considered a bad idea because it causes data shuffling to one node and so we lose parallelism.
 * See: http://spark.apache.org/docs/latest/programming-guide.html
 */
public final class SparkFileUtils
{
	private SparkFileUtils()
	{
	}

	public static void renameFile(String path, String fileName)
	{
		Properties props = new Properties();

		try
		{
			// Reading the .properties file...
			FileInputStream propsFile = new FileInputStream("conf/spark-backend.properties");
			props.load(propsFile);
		}
		catch(IOException e)
		{
			System.out.println("Error reading spark-backend properties file.");

			e.printStackTrace();
		}

		File folder = new File(path);

		File[] files = folder.listFiles();

		for(int i = 0; i < files.length; i++)
		{
			if(files[i].isFile())
			{
				if(files[i].getName().startsWith("part"))
				{
					File file = files[i];

					Path source = file.toPath();
					System.out.println("Source file = " + source.toString());

					String csvPath = props.getProperty("csv.Path");
					System.out.println("Path to save CSV = " + csvPath);

					// Rename and move the single CSV file from Spark [see
					// DataFrame.coalesce(1)] to the
					// output folder called csv
					// It could be Merge instead if we were not using coalesce...
					Path to = new File(csvPath + "/" + fileName).toPath();
					System.out.println("Destination file = " + to.toString());

					try
					{
						CopyOption[] options = new CopyOption[] { StandardCopyOption.REPLACE_EXISTING,
								StandardCopyOption.COPY_ATTRIBUTES };

						Files.copy(source, to, options);

						System.out.println("File renamed and copied successfully.");
					}
					catch(IOException e)
					{
						System.out.println("Error renaming and copying file.");

						e.printStackTrace();
					}
				}
			}
			else if(files[i].isDirectory())
			{
				System.out.println("Directory " + files[i].getName());
			}
		}
	}
}