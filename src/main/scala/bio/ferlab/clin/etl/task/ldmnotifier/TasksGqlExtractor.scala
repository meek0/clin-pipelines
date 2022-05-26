package bio.ferlab.clin.etl.task.ldmnotifier

import bio.ferlab.clin.etl.task.ldmnotifier.model.{Data, GqlResponse, Task}
import play.api.libs.json._
import sttp.client3.{HttpURLConnectionBackend, Identity, Response, UriContext, basicRequest}
import sttp.model.MediaType

object TasksGqlExtractor {
  private type TasksResponse = Identity[Response[Either[String, String]]]

  //Needed right now to make graphql queries work.
  // If the graphql query generates more results than this <upper bound> it will generate an error.
  val GQL_COUNT_UPPER_BOUND = 1000

  def buildGqlTasksQueryHttpPostBody(runName: String): String = {
    val query =
      s"""
         |{
         |  taskList: TaskList(run_name: "$runName") {
         |    id
         |    focus @flatten {
         |      serviceRequestReference: reference
         |    }
         |    requester @flatten {
         |      requester: resource(type: Organization) {
         |        id
         |        alias @first @singleton
         |        contact @flatten @first @singleton {
         |          telecom @flatten @first @singleton {
         |            email: value
         |          }
         |        }
         |      }
         |    }
         |    output @flatten {
         |	    valueReference @flatten {
         |		    documents: resource(type: DocumentReference) {
         |          contentList: content {
         |					  attachment {
         |						  url
         |              hash64: hash
         |              title
         |		  		   }
         |             format @flatten {
         |              fileFormat: code
         |             }
         |				  }
         |          context @flatten {
         |            related@first @flatten {
         |              sample:resource @flatten {
         |                accessionIdentifier @flatten { sampleId: value }
         |              }
         |            }
         |           }
         |           subject @flatten { patientReference: reference }
         |           type @flatten {
         |            coding @first @flatten { fileType: code }
         |            }
         |				}
         |      }
         |    }
         |  }
         |}
         |""".stripMargin
    s"""{ "query": ${JsString(query)} }"""
  }

  def fetchTasksFromFhir(baseUrl: String, token: String, runName: String): TasksResponse = {
    val backend = HttpURLConnectionBackend()
    val response = basicRequest
      .headers(Map("Authorization" -> s"Bearer $token"))
      .contentType(MediaType.ApplicationJson)
      .body(buildGqlTasksQueryHttpPostBody(runName))
      .post(uri"$baseUrl/${"$graphql"}?_count=$GQL_COUNT_UPPER_BOUND")
      .send(backend)
    backend.close
    response
  }

  def checkIfGqlResponseHasData(json: JsValue): Either[String, Seq[Task]] = {
    json.validate[GqlResponse].fold(
      invalid => {
        Left(s"Errors: ${JsError.toJson(invalid)}")
      },
      valid => {
        valid.data match {
          case Some(Data(Some(taskList))) if taskList.nonEmpty => Right(taskList)
          case Some(_) => Left("Task list is empty")
          case _ => Left(s"Errors: No data for task")
        }
      },
    )
  }
}
