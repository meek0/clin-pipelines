package bio.ferlab.clin.etl.task.fileimport.model

sealed trait SpecimenSampleType {
  def level: String
}

sealed trait SampleAliquotType extends SpecimenSampleType {
  def parentType: SpecimenSampleType
}

case object SpecimenType extends SpecimenSampleType {
  override def toString: String = "Specimen"

  override def level: String = "specimen"
}

case object SampleType extends SampleAliquotType {
  override def toString: String = "Sample"

  override def level: String = "sample"

  override def parentType: SpecimenSampleType = SpecimenType
}

case object AliquotType extends SampleAliquotType {
  override def toString: String = "Aliquot"

  override def level: String = "aliquot"

  override def parentType: SpecimenSampleType = SampleType
}