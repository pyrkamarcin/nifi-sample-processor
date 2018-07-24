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
import java.util.HashMap
import org.apache.nifi.flowfile.FlowFile


@Tags("example", "test")
@CapabilityDescription("Provide a description")
@SeeAlso
@ReadsAttributes(ReadsAttribute(attribute = "", description = ""))
@WritesAttributes(WritesAttribute(attribute = "", description = ""))

data class Identity(val username: String, val password: String)

class MyProcessor : AbstractProcessor() {
    private var descriptors: List<PropertyDescriptor>? = null

    private var relationships: Set<Relationship>? = null

    private val mapper = jacksonObjectMapper()

    override fun getRelationships(): Set<Relationship>? {
        return this.relationships
    }

    override fun getSupportedPropertyDescriptors(): List<PropertyDescriptor>? {
        return descriptors
    }

    companion object {

        val POOL_ID = PropertyDescriptor.Builder().name("POOL_ID")
                .displayName("POOL_ID")
                .description("")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build()!!
        val CLIENTAPP_ID = PropertyDescriptor.Builder().name("CLIENTAPP_ID")
                .displayName("CLIENTAPP_ID")
                .description("")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build()!!
        val FED_POOL_ID = PropertyDescriptor.Builder().name("FED_POOL_ID")
                .displayName("FED_POOL_ID")
                .description("")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build()!!
        val CUSTOMDOMAIN = PropertyDescriptor.Builder().name("CUSTOMDOMAIN")
                .displayName("CUSTOMDOMAIN")
                .description("")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build()!!
        val REGION = PropertyDescriptor.Builder().name("REGION")
                .displayName("REGION")
                .description("")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build()!!

        val SUCCESS = Relationship.Builder()
                .name("SUCCESS")
                .description("SUCCESS relationship")
                .build()!!

        val FAILURE = Relationship.Builder()
                .name("FAILURE")
                .description("FAILURE relationship")
                .build()!!
    }

    override fun init(context: ProcessorInitializationContext?) {
        val descriptors = ArrayList<PropertyDescriptor>()
        descriptors.add(POOL_ID)
        descriptors.add(CLIENTAPP_ID)
        descriptors.add(FED_POOL_ID)
        descriptors.add(CUSTOMDOMAIN)
        descriptors.add(REGION)
        this.descriptors = Collections.unmodifiableList(descriptors)

        val relationships = HashSet<Relationship>()
        relationships.add(SUCCESS)
        relationships.add(FAILURE)
        this.relationships = Collections.unmodifiableSet(relationships)
    }

    @Throws(ProcessException::class)
    override fun onTrigger(context: ProcessContext, session: ProcessSession) {
        val flowFile = session.get() ?: return
        val attributes = HashMap<String, String>()

        var outgoingFlowFile = session.write(flowFile) { input, output ->
            val json: String = input.bufferedReader().use { it.readText() }
            val identity: Identity = mapper.readValue(json)

            val POOL_ID = getPropertyValue(POOL_ID, context, flowFile)
            val CLIENTAPP_ID = getPropertyValue(CLIENTAPP_ID, context, flowFile)
            val FED_POOL_ID = getPropertyValue(FED_POOL_ID, context, flowFile)
            val CUSTOMDOMAIN = getPropertyValue(CUSTOMDOMAIN, context, flowFile)
            val REGION = getPropertyValue(REGION, context, flowFile)

            val cognitoHelper = CognitoHelper(
                    POOL_ID,
                    CLIENTAPP_ID,
                    FED_POOL_ID,
                    CUSTOMDOMAIN,
                    REGION
            )

            val response = cognitoHelper.signInUser(identity.username, identity.password)
            output.write(response.toByteArray())
        }

        attributes[CoreAttributes.MIME_TYPE.key()] = "application/json"

        outgoingFlowFile = session.putAllAttributes(outgoingFlowFile, attributes)
        session.transfer(outgoingFlowFile, SUCCESS)
    }
}

private fun getPropertyValue(property: PropertyDescriptor, context: ProcessContext, flowfile: FlowFile): String {
    return context.getProperty(property).evaluateAttributeExpressions(flowfile).value
}