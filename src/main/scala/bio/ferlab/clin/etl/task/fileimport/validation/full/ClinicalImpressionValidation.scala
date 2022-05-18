package bio.ferlab.clin.etl.task.fileimport.validation.full

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.task.fileimport.model.{FullAnalysis, TClinicalImpression}
import cats.implicits.catsSyntaxValidatedId

object ClinicalImpressionValidation {

  def validateClinicalImpression(a: FullAnalysis): ValidationResult[TClinicalImpression] = TClinicalImpression(a).validNel[String]

}
