package bio.ferlab.clin.etl.fhir

import java.util.{Date, Locale}

object Model {

  object ClinServiceRequestCode extends Enumeration {
    val WXS = Value("WXS")
    val WGS = Value("WGS")
    val GP = Value("GP")

    implicit class Display(specimenType: Value) {
      def display: String = specimenType match {
        case WXS => "Whole Exome Sequencing"
        case WGS => "Whole Genome Sequencing"
        case GP => "Gene Panel"
        case _ => "Other"
      }

      def definition: String = specimenType match {
        case WXS => "Process of determining the complete DNA sequence of the genome at a single time"
        case WGS => "Process of determining all of the exons within the genome sequence at a single time"
        case GP => "Process of determining the complete sequences of the indicated gene panel at a single time"
        case _ => "Other"
      }

      def designation(locale: Locale): String = locale match {
        case Locale.FRENCH => {
          specimenType match {
            case WXS => "Séquençage du Génome Entier"
            case WGS => "Séquençage de l'Exome Entier"
            case GP => "Panel de Gènes"
            case _ => "Other"
          }
        }
        case _: Locale => {
          specimenType match {
            case WXS => "Whole Genome Sequencing"
            case WGS => "Whole Exome Sequencing"
            case GP => "Gene panel"
            case _ => "Other"
          }
        }
      }
    }
  }

  object ClinExtensionValueType extends Enumeration {
    val STRING, DATE, INTEGER, DECIMAL, BOOLEAN, CODE = Value
  }

  object AnalysisType extends Enumeration {
    val SEQUENCING_ALIGNMENT, VARIANT_CALLING, QC_REPORT = Value

    implicit class Display(analysisType: Value) {
      def display: String = analysisType match {
        case SEQUENCING_ALIGNMENT => "Sequencing Alignment"
        case VARIANT_CALLING => "Variant Calling"
        case QC_REPORT => "QC Report"
        case _ => "Other"
      }

      def text: String = analysisType match {
        case SEQUENCING_ALIGNMENT => "Sequencing Alignment"
        case VARIANT_CALLING => "Variant Calling"
        case QC_REPORT => "QC Report"
        case _ => "Other"
      }
    }
  }

  object AnalysisRelationship extends Enumeration {
    val PARENT = Value

    implicit class Display(analysisType: Value) {
      def display: String = analysisType match {
        case PARENT => "Parent analysis"
        case _ => "Other"
      }
    }
  }

  case class Terminology(system: String, code: String, display: String, text: Option[String])
  case class Designation(language: Locale, value: String)
  case class FileInfo(contentType: String, url: String, size: Int, hash: String, title: String, creationDate: Date)
  case class ClinExtension(url:String, value:String, valueType:ClinExtensionValueType.Value)
}
