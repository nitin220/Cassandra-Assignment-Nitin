import org.apache.log4j.Logger

object VideoSystem extends App {

  val log = Logger.getLogger(this.getClass)

  val obj = new DatabaseSystem
  val res = obj.createTable

  log.info(s"$res \n")

  obj.insertIntoUser("nitin@knoldus.in","Nitin123",1)

  obj.insertIntoVideo(1,"Bin-Tere(Acoustic-Version)",1,2017)
  obj.insertIntoVideo(2,"Teri-Yadeen(Acoustic-Version)",1,2016)

  obj.insertIntoUser("nitin@gmail.com","Nitin321",2)

  obj.insertIntoVideo(3,"Tum-hi-ho(Acoustic-Version)",2,2016)
  obj.insertIntoVideo(4,"Milne-hai-Mujhse(Acoustic-Version)",2,2015)

  obj.insertIntoUser("nitin20.garg@gmail.com","Nitin12344",3)

  obj.insertIntoVideo(5,"Shyam-Shavere-Tri(Acoustic-Version)",3,2012)
  obj.insertIntoVideo(6,"AAbhija(Acoustic-Version)",3,2014)

  log.info("Query to find user by email")
  obj.userByEmail("nitin@knoldus.in")

  log.info("\nQuery to find video by name")
  obj.videoByName("Teri-Yadeen(Acoustic-Version)")

  log.info("\nQuery to find videos uploaded after 2015")
  obj.videoAfterYear(2015)

  log.info("\nQuery to find video by user_id and records should be selected in descending order by year")
  obj.videoByUserIdAndYear(1,2015)







}
