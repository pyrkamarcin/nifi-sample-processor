package processors

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.annotation.behavior.ReadsAttribute
import org.apache.nifi.annotation.behavior.ReadsAttributes
import org.apache.nifi.annotation.behavior.WritesAttribute
import org.apache.nifi.annotation.behavior.WritesAttributes
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.SeeAlso
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.ProcessorInitializationContext
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.util.StandardValidators

import com.fasterxml.jackson.module.kotlin.*
import java.util.*

@Tags("example", "test")
@CapabilityDescription("Provide a description")
@SeeAlso
@ReadsAttributes(ReadsAttribute(attribute = "", description = ""))
@WritesAttributes(WritesAttribute(attribute = "", description = ""))

data class Person(val name: String, val age: Int, var gender: String?)

class MyProcessor : AbstractProcessor() {
    private var descriptors: List<PropertyDescriptor>? = null

    private var relationships: Set<Relationship>? = null

    private val mapper = jacksonObjectMapper()

    override fun init(context: ProcessorInitializationContext?) {
        val descriptors = ArrayList<PropertyDescriptor>()
        descriptors.add(MY_PROPERTY)
        this.descriptors = Collections.unmodifiableList(descriptors)

        val relationships = HashSet<Relationship>()
        relationships.add(SUCCESS)
        this.relationships = Collections.unmodifiableSet(relationships)
    }

    override fun getRelationships(): Set<Relationship>? {
        return this.relationships
    }

    override fun getSupportedPropertyDescriptors(): List<PropertyDescriptor>? {
        return descriptors
    }

    @Throws(ProcessException::class)
    override fun onTrigger(context: ProcessContext, session: ProcessSession) {
        val flowFile = session.get() ?: return
        val attributes = HashMap<String, String>()

        var outgoingFlowFile = session.write(flowFile) { input, output ->
            val json: String = input.bufferedReader().use { it.readText() }
            val person: Person = mapper.readValue(json)
            person.gender = "male"
            val newJson: String = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(person)
            logger.info("Successfully regenerate {} class", arrayOf("Person"))
            output.write(newJson.toByteArray())
        }

        attributes[CoreAttributes.MIME_TYPE.key()] = "application/json"

        outgoingFlowFile = session.putAllAttributes(outgoingFlowFile, attributes)
        session.transfer(outgoingFlowFile, SUCCESS)
    }

    companion object {

        val MY_PROPERTY = PropertyDescriptor.Builder().name("MY_PROPERTY")
                .displayName("My property")
                .description("Example Property")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build()!!

        val SUCCESS = Relationship.Builder()
                .name("SUCCESS")
                .description("Success relationship")
                .build()!!
    }
}
