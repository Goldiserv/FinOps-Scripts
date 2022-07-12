package com.myorg;

import software.constructs.Construct;
import software.amazon.awscdk.Duration;

import software.amazon.awscdk.services.sqs.Queue;

import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;

import software.amazon.awscdk.services.apigatewayv2.alpha.HttpApi;
import software.amazon.awscdk.services.apigatewayv2.alpha.HttpApiProps;
import software.amazon.awscdk.services.apigatewayv2.alpha.AddRoutesOptions;
import software.amazon.awscdk.services.apigatewayv2.alpha.HttpApiProps;
import software.amazon.awscdk.services.apigatewayv2.alpha.HttpMethod;
import software.amazon.awscdk.services.apigatewayv2.alpha.HttpRoute;
import software.amazon.awscdk.services.apigatewayv2.alpha.PayloadFormatVersion;
import software.amazon.awscdk.services.apigatewayv2.integrations.alpha.HttpLambdaIntegration;
import software.amazon.awscdk.services.apigatewayv2.integrations.alpha.HttpLambdaIntegrationProps;

import software.amazon.awscdk.services.apigatewayv2.alpha.ThrottleSettings;

import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.FunctionProps;
import software.amazon.awscdk.services.lambda.Runtime;

import software.amazon.awscdk.services.logs.RetentionDays;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;

// import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.github.cdimascio.dotenv.Dotenv;

// val dotenv = dotenv()
// dotenv["MY_ENV_VAR1"]
public class CdkStack extends Stack {
    public CdkStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public CdkStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        // example resource
        // final Queue queue = Queue.Builder.create(this, "CdkQueue")
        // .visibilityTimeout(Duration.seconds(300))
        // .build();

        //better practice to use KMS
        Dotenv dotenv = Dotenv.load();
        // System.out.println(dotenv.get("DATADOG_SITE"));
        Map<String, String> lambdaEnvMap = new HashMap<>();
        lambdaEnvMap.put("DD_API_KEY", dotenv.get("DD_API_KEY"));
        lambdaEnvMap.put("DD_APP_KEY", dotenv.get("DD_APP_KEY"));
        lambdaEnvMap.put("DATADOG_SITE", dotenv.get("DATADOG_SITE"));

        Function functionGetPodUtil = new Function(this, "getPodUtil",
                getLambdaFunctionProps(lambdaEnvMap, "getpodutil.App"));
        
        // Unsure where to put throttleSettings
        // ThrottleSettings throttleSettings = ThrottleSettings.builder()
        //         .burstLimit(123)
        //         .rateLimit(123)
        //         .build();

        HttpApi httpApi = new HttpApi(this, "datadog-api", HttpApiProps.builder()
                .apiName("datadog-api")
                // .throttleSettings(throttleSettings) //doesn't work here
                .build());

        HttpLambdaIntegration lambdaIntegration = new HttpLambdaIntegration("dd-int", functionGetPodUtil);
        List<HttpRoute> routes = httpApi.addRoutes(AddRoutesOptions.builder()
                .path("/dd/{id}")
                .methods(List.of(HttpMethod.GET))
                .integration(lambdaIntegration)
                .build());

        new CfnOutput(this, "HttApi", CfnOutputProps.builder()
                .description("Url for Http Api")
                .value(httpApi.getApiEndpoint())
                .build());
        }

    private FunctionProps getLambdaFunctionProps(Map<String, String> lambdaEnvMap, String handler) {
        return FunctionProps.builder()
                .code(Code.fromAsset("../assets/GetPodUtil.jar"))
                .handler(handler)
                .runtime(Runtime.JAVA_11)
                .environment(lambdaEnvMap)
                .timeout(Duration.seconds(15))
                .memorySize(512)
                .logRetention(RetentionDays.ONE_WEEK)
                .build();
    }
}
