import com.datastax.driver.core.Cluster
import org.apache.log4j.Logger

class DatabaseSystem {

  val log = Logger.getLogger(this.getClass)

  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()

  val session = cluster.connect("video_system")

  def createTable: String = {

    session.execute("CREATE TABLE user (email text, password text, user_id int, PRIMARY KEY (email,user_id))")

    session.execute("CREATE TABLE video (video_id int,video_name text, user_id int, year int, PRIMARY KEY ((video_name),year))")

    session.execute("CREATE TABLE videoByUserIdAndYear (video_id int,video_name text, user_id int, year int, PRIMARY KEY (user_id,year))")

    "Tables Created"
  }

  def insertIntoUser(email: String, password: String, id: Int) = {

    session.execute(s"INSERT INTO user (email , user_id , password ) VALUES ( '$email',$id,'$password');")

  }

  def insertIntoVideo(videoId: Int, videoName: String, userId: Int, year: Int) = {

    session.execute(s"INSERT INTO video (video_id , video_name , user_id , year ) VALUES ($videoId,'$videoName',$userId,$year )")

    session.execute(s"INSERT INTO videoByUserIdAndYear (video_id , video_name , user_id , year ) VALUES ($videoId,'$videoName',$userId,$year )")

  }

  def userByEmail(email: String) = {

    val res = session.execute(s"SELECT * FROM user where email='$email'")

    val listOfResult = res.all.toArray.toList

    listOfResult.map(row => log.info(row))

  }

  def videoByName(name: String) = {

    val res = session.execute(s"SELECT * FROM video where video_name='$name'")

    val listOfResult = res.all.toArray.toList

    listOfResult.map(row => log.info(row))

  }

  def videoAfterYear(year: Int) = {
    val res = session.execute(s"SELECT * FROM video where year>$year ALLOW FILTERING")

    val listOfResult = res.all.toArray.toList

    listOfResult.map(row => log.info(row))

  }

  def videoByUserIdAndYear(id: Int, year: Int) = {

    val res = session.execute(s"SELECT * FROM videoByUserIdAndYear WHERE user_id =$id AND year >=$year ORDER BY year DESC ;")

    val listOfResult = res.all.toArray.toList

    listOfResult.map(row => log.info(row))

    cluster.close()
  }


}




