package json.schema.pojo

import java.util.HashMap
import java.util.List
import java.util.Map
import com.fasterxml.jackson.annotation.JsonAnyGetter
import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import scala.beans.{BeanProperty, BooleanBeanProperty}
//remove if not needed
import scala.collection.JavaConversions._

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(Array("url", "name", "description", "productionCost", "combatStrength", "moves", "range", "rangedStrength", "technology", "upgradesTo", "notes"))
class Canon {

  @JsonProperty("url")
  @BeanProperty
  var url: String = _

  @JsonProperty("name")
  @BeanProperty
  var name: String = _

  @JsonProperty("description")
  @BeanProperty
  var description: String = _

  @JsonProperty("productionCost")
  @BeanProperty
  var productionCost: java.lang.Integer = _

  @JsonProperty("combatStrength")
  @BeanProperty
  var combatStrength: java.lang.Integer = _

  @JsonProperty("moves")
  @BeanProperty
  var moves: java.lang.Integer = _

  @JsonProperty("range")
  @BeanProperty
  var range: java.lang.Integer = _

  @JsonProperty("rangedStrength")
  @BeanProperty
  var rangedStrength: java.lang.Integer = _

  @JsonProperty("technology")
  @BeanProperty
  var technology: String = _

  @JsonProperty("upgradesTo")
  @BeanProperty
  var upgradesTo: String = _

  @JsonProperty("notes")
  @BeanProperty
  var notes: List[String] = null

  @JsonIgnore
  @BeanProperty
  var additionalProperties: Map[String, Any] = new HashMap[String, Any]()

  @JsonAnySetter
  def setAdditionalProperty(name: String, value: AnyRef) {
    this.additionalProperties.put(name, value)
  }
}