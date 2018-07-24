package processors

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.AnonymousAWSCredentials
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.cognitoidentity.model.*
import com.amazonaws.services.cognitoidp.AWSCognitoIdentityProviderClientBuilder
import com.amazonaws.services.cognitoidp.model.*
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import java.io.IOException
import java.io.InputStream
import java.util.*
import java.util.ArrayList
import com.amazonaws.services.cognitoidp.model.SignUpRequest
import com.amazonaws.services.cognitoidp.model.ConfirmSignUpRequest

class CognitoHelper(private var POOL_ID: String, private var CLIENTAPP_ID: String, private var FED_POOL_ID: String, private var CUSTOMDOMAIN: String, private var REGION: String) {

    fun SignUpUser(username: String, password: String, email: String, phonenumber: String): Boolean {
        val awsCreds = AnonymousAWSCredentials()
        val cognitoIdentityProvider = AWSCognitoIdentityProviderClientBuilder
                .standard()
                .withCredentials(AWSStaticCredentialsProvider(awsCreds))
                .withRegion(Regions.fromName(REGION))
                .build()

        val signUpRequest = SignUpRequest()
        signUpRequest.clientId = CLIENTAPP_ID
        signUpRequest.username = username
        signUpRequest.password = password
        val list = ArrayList<AttributeType>()

        val attributeType = AttributeType()
        attributeType.name = "phone_number"
        attributeType.value = phonenumber
        list.add(attributeType)

        val attributeType1 = AttributeType()
        attributeType1.name = "email"
        attributeType1.value = email
        list.add(attributeType1)

        signUpRequest.setUserAttributes(list)

        try {
            val result = cognitoIdentityProvider.signUp(signUpRequest)
            println(result)
        } catch (e: Exception) {
            println(e)
            return false
        }

        return true
    }

    fun signInUser(username: String, password: String): String {
        val awsCreds = AnonymousAWSCredentials()
        val cognitoIdentityProvider = AWSCognitoIdentityProviderClientBuilder
                .standard()
                .withCredentials(AWSStaticCredentialsProvider(awsCreds))
                .withRegion(Regions.fromName(REGION))
                .build()

        val signInRequest = InitiateAuthRequest()
        signInRequest.clientId = CLIENTAPP_ID
        signInRequest.authFlow = "USER_PASSWORD_AUTH"

        val list: MutableMap<String, String> = mutableMapOf()

        list["USERNAME"] = username
        list["PASSWORD"] = password

        signInRequest.authParameters = list

        try {
            val result = cognitoIdentityProvider.initiateAuth(signInRequest)
            return result.toString().replace("(?!\\d+[^\\w])([\\w-\\.]+)".toRegex(), "\"$1\"").replace(",}}", "}}")
        } catch (e: Exception) {
            return e.toString().replace("(?!\\d+[^\\w])([\\w-\\.]+)".toRegex(), "\"$1\"").replace(",}}", "}}")
        }
    }
}