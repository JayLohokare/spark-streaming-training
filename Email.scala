package in.skylinelabs.spark

import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity

object Email {

  def main(args: Array[String]) {
    val to = "jaylohokare@gmail.com"
    val message = "Message"
    val subject = "Subject"
    val url = "http://skylinelabs.in/connectx/email.php";
    val finalMessage = "Yaaayy"

    val post = new HttpPost(url)
    val client = new DefaultHttpClient

    val nameValuePairs = new ArrayList[NameValuePair](2)
    nameValuePairs.add(new BasicNameValuePair("to", to));
    nameValuePairs.add(new BasicNameValuePair("subject", subject));
    nameValuePairs.add(new BasicNameValuePair("message", message + "\n\n" + "Message obtained from device is : \n" + finalMessage));
    post.setEntity(new UrlEncodedFormEntity(nameValuePairs));

    val response = client.execute(post)

  }
}