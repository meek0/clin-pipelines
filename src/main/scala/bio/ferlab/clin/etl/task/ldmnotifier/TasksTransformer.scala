package bio.ferlab.clin.etl.task.ldmnotifier

import bio.ferlab.clin.etl.task.ldmnotifier.model.{Attachments, Task}

object TasksTransformer {
  type AliasUrlValues = (String, Seq[String])

  type AliasToUrlValues = Map[String, Seq[String]]

  type AliasToEmailAddress = Map[String, String]

  private def mapAttachmentsToUrlValues(attachments: Attachments): Seq[String] = {
    attachments.urls.map(_.url)
  }

  private def retainOnlyDistinctUrlValuesForEachGroup(aliasToUrlsForAllTasks: Seq[AliasUrlValues])= {
    val flattenedUrlValues = aliasToUrlsForAllTasks.flatMap(_._2)
    flattenedUrlValues.distinct
  }

  def groupAttachmentUrlsByEachOfOwnerAliases(tasks: Seq[Task]): AliasToUrlValues = {
    val aliasesToUrlsFromEachTask = tasks.map(task => (task.owner.alias, mapAttachmentsToUrlValues(task.attachments)))
    aliasesToUrlsFromEachTask
      .groupBy(_._1)
      .mapValues(retainOnlyDistinctUrlValuesForEachGroup)
  }

  def mapLdmAliasToEmailAddress(tasks: Seq[Task]): AliasToEmailAddress = {
    tasks.map(task => (task.owner.alias, task.owner.email)).toMap
  }
}
