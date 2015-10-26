package com.yookos.migration;

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._
import org.apache.commons.lang.StringEscapeUtils

case class Profile(biography: Option[String], 
                  creationdate: String, 
                  lastupdated: String,
                  relationshipstatus: Option[String],
                  username: String,
                  hometown: Option[String],
                  firstname: Option[String],
                  timezone: Option[String],
                  currentcity: Option[String],
                  userid: String,
                  lastname: Option[String],
                  birthdate: Option[String],
                  homecountry: Option[String],
                  title: Option[String],
                  currentcountry: Option[String],
                  terms: Boolean,
                  gender: Option[String],
                  imageurl: Option[String],
                  //phonenumber: Option[String],
                  //mobile: Option[String],
                  homeaddress: Option[String],
                  location: Option[String]
                  ) extends Serializable

object Profile {
  implicit object Mapper extends DefaultColumnMapper[Profile](
    Map("biography" -> "biography", 
      "creationdate" -> "creationdate",
      "lastupdated" -> "lastupdated",
      "relationshipstatus" -> "relationshipstatus",
      "username" -> "username",
      "hometown" -> "hometown",
      "firstname" -> "firstname",
      "timezone" -> "timezone",
      "currentcity" -> "currentcity",
      "userid" -> "userid",
      "lastname" -> "lastname",
      "birthdate" -> "birthdate",
      "homecountry" -> "homecountry",
      "title" -> "title",
      "currentcountry" -> "currentcountry",
      "terms" -> "terms",
      "gender" -> "gender",
      "imageurl" -> "imageurl",
      //"phonenumber" -> "phonenumber",
      //"mobile" -> "mobile",
      "homeaddress" -> "homeaddress",
      "location" -> "location"
      ))

  val cs = this.getClass.getConstructors
  def createFromList(params: List[Any]): Profile = params match {
    case List(biography: Any, 
      creationdate: Any, 
      lastupdated: Any,
      relationshipstatus: Any,
      username: Any,
      hometown: Any,
      firstname: Any,
      timezone: Any,
      currentcity: Any,
      userid: Any,
      lastname: Any,
      birthdate: Any,
      homecountry: Any,
      title: Any,
      currentcountry: Any,
      terms: Boolean,
      gender: Any,
      imageurl: Any,
      //phonenumber: Any,
      //mobile: Any,
      homeaddress: Any,
      location: Any
      ) => Profile(
        Some(StringEscapeUtils.escapeJava(biography.asInstanceOf[String])), 
        StringEscapeUtils.escapeJava(creationdate.asInstanceOf[String]), 
        StringEscapeUtils.escapeJava(lastupdated.asInstanceOf[String]),
        Some(StringEscapeUtils.escapeJava(relationshipstatus.asInstanceOf[String])),
        StringEscapeUtils.escapeJava(username.asInstanceOf[String]), 
        Some(StringEscapeUtils.escapeJava(hometown.asInstanceOf[String])),
        Some(StringEscapeUtils.escapeJava(firstname.asInstanceOf[String])),
        Some(StringEscapeUtils.escapeJava(timezone.asInstanceOf[String])),
        Some(StringEscapeUtils.escapeJava(currentcity.asInstanceOf[String])), 
        StringEscapeUtils.escapeJava(userid.asInstanceOf[String]), 
        Some(StringEscapeUtils.escapeJava(lastname.asInstanceOf[String])),
        Some(StringEscapeUtils.escapeJava(birthdate.asInstanceOf[String])),
        Some(StringEscapeUtils.escapeJava(homecountry.asInstanceOf[String])),
        Some(StringEscapeUtils.escapeJava(title.asInstanceOf[String])),
        Some(StringEscapeUtils.escapeJava(currentcountry.asInstanceOf[String])),
        terms.booleanValue(),
        Some(StringEscapeUtils.escapeJava(gender.asInstanceOf[String])),
        Some(imageurl.asInstanceOf[String]),
        //Some(StringEscapeUtils.escapeJava(phonenumber.asInstanceOf[String])),
        //Some(StringEscapeUtils.escapeJava(mobile.asInstanceOf[String])),
        Some(StringEscapeUtils.escapeJava(homeaddress.asInstanceOf[String])),
        Some(StringEscapeUtils.escapeJava(location.asInstanceOf[String]))
      )
  }
}
