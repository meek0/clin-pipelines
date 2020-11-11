package fhir

import fhir.Model.{ClinExtension, ClinExtensionValueType}
import org.hl7.fhir.r4.model.{BooleanType, CodeType, DateType, DecimalType, Extension, IntegerType, StringType}

package object Utils {
  def createExtension(url: String, values: Seq[ClinExtension]): Extension = {
    val ext:Extension = new Extension()
    ext.setUrl(url)

    values.foreach(metric => {
      metric.valueType match {
        case ClinExtensionValueType.DECIMAL => {
          ext.addExtension(metric.url, new DecimalType(metric.value))
        }
        case ClinExtensionValueType.INTEGER => {
          ext.addExtension(metric.url, new IntegerType(metric.value))
        }
        case ClinExtensionValueType.BOOLEAN => {
          ext.addExtension(metric.url, new BooleanType(metric.value))
        }
        case ClinExtensionValueType.DATE => {
          ext.addExtension(metric.url, new DateType(metric.value))
        }
        case ClinExtensionValueType.STRING => {
          ext.addExtension(metric.url, new StringType(metric.value))
        }
        case ClinExtensionValueType.CODE => {
          ext.addExtension(metric.url, new CodeType(metric.value))
        }
        case _ => {
          ext.addExtension(metric.url, new StringType(metric.value))
        }
      }
    })

    ext
  }
}
