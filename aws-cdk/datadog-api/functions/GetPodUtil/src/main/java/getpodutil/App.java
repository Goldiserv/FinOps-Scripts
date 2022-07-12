package getpodutil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Map;
import java.util.stream.Collectors;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

import com.datadog.api.v1.client.ApiClient;
import com.datadog.api.v1.client.ApiException;
import com.datadog.api.v1.client.Configuration;

import com.datadog.api.v1.client.api.MetricsApi;
import com.datadog.api.v1.client.model.MetricsQueryMetadata;
import com.datadog.api.v1.client.model.MetricsQueryResponse;
import com.datadog.api.v1.client.model.MetricsQueryUnit;

import com.google.gson.Gson;

import static software.amazon.lambda.powertools.tracing.CaptureMode.DISABLED;

/**
 * Handler for requests to Lambda function.
 */
public class App implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    @Tracing(captureMode = DISABLED)
    @Metrics(captureColdStart = true)

    static MetricsQueryResponse queryData(String queryString) {
        // code to be executed

        HashMap<String, String> secrets = new HashMap<>();
        secrets.put("apiKeyAuth", System.getenv("DD_API_KEY"));
        secrets.put("appKeyAuth", System.getenv("DD_APP_KEY"));

        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.configureApiKeys(secrets);
        defaultClient.setServerVariables(Collections.singletonMap("site", System.getenv("DATADOG_SITE")));

        MetricsApi apiInstance = new MetricsApi(defaultClient);

        try {
            MetricsQueryResponse result = apiInstance.queryMetrics(
                    OffsetDateTime.now().plusDays(-1).toInstant().getEpochSecond(),
                    OffsetDateTime.now().toInstant().getEpochSecond(),
                    queryString);

            System.out.println(result);
            return result;
        } catch (ApiException e) {
            System.err.println("Exception when calling MetricsApi#queryMetrics");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
            return null;
        }
    }

    static List<Object[]> formatResponseData(MetricsQueryResponse resp) {
        // Gson g = new Gson();

        // MetricsQueryResponse metricsQueryResponse = g.fromJson("{\"name\":
        // \"John\"}", MetricsQueryResponse.class);
        // System.out.println(metricsQueryResponse.getSeries()); //John
        // System.out.println(g.toJson(metricsQueryResponse)); // {"name":"John"}

        List<Object[]> result_holder = new ArrayList<Object[]>();

        for (MetricsQueryMetadata metricData : resp.getSeries()) {

            // statements using var;
            String metric = metricData.getMetric();
            List<MetricsQueryUnit> unit_info = metricData.getUnit();
            Double scale_factor = unit_info.get(0).getScaleFactor();

            List<List<Double>> pointlist = metricData.getPointlist();
            // e.g. pointlist = [[1.6575123E12, 88.795056993691]]

            // determine count,max,avg of datadog record
            List<Double> p_builder = new ArrayList<Double>();
            for (List<Double> p : pointlist) {
                Double x = p.get(1);
                if (x != null)
                    p_builder.add(x);
            }
            int metric_len = p_builder.size();
            Double metric_avg = p_builder
                    .stream()
                    .mapToDouble(a -> a)
                    .average().getAsDouble() * scale_factor;
            Double metric_max = p_builder
                    .stream()
                    .mapToDouble(a -> a)
                    .max().getAsDouble() * scale_factor;

            Object[] o = new Object[] {
                    metric,
                    metric_len,
                    metric_max,
                    metric_avg
            };

            result_holder.add(o);

        }
        return result_holder;
    }

    static String buildQuery(String pod_name) {
        // static String buildQuery(String pod_name, String aggregation_secs) {

        StringBuilder queryString = new StringBuilder();
        /*
         * Note: rollup defines a custom aggregation that may be produce incorrect
         * values e.g. max value that's higher than the true max
         * example rollup function:
         * queryString.append(String.format(
         * "max:kubernetes.cpu.requests{pod_name:%s} by {pod_name}.rollup(max, %s) ",
         * pod_name, aggregation_secs))
         */
        queryString.append(String.format(
                "max:kubernetes.memory.usage{pod_name:%s} by {pod_name}, ",
                pod_name));
        queryString.append(String.format(
                "max:kubernetes.memory.limits{pod_name:%s} by {pod_name}, ",
                pod_name));
        queryString.append(String.format(
                "max:kubernetes.memory.usage_pct{pod_name:%s} by {pod_name}, ",
                pod_name));
        queryString.append(String.format(
                "max:kubernetes.cpu.usage.total{pod_name:%s} by {pod_name}, ",
                pod_name));
        queryString.append(String.format(
                "max:kubernetes.cpu.requests{pod_name:%s} by {pod_name} ",
                pod_name));

        System.out.println(queryString.toString());

        return queryString.toString();

    }

    static Object[] getCpuUtil(Object[] cpu_usage_data, Object[] cpu_requests_data) {
        // e.g. cpu_usage_data = ['kubernetes.cpu.usage.total', 15, 4.771e-06,
        // 3.496e-06]
        // e.g. cpu_requests_data = ['kubernetes.cpu.requests', 15,
        // 0.0010000000474974513,
        // 0.0010000000474974513]
        // String[] col_headings = new String[] {"Metric", "Count", "Max", "Avg"};

        Double max_util = Double.parseDouble(cpu_usage_data[2].toString())
                / Double.parseDouble(cpu_requests_data[2].toString());
        if (max_util == Double.POSITIVE_INFINITY || max_util == Double.NEGATIVE_INFINITY)
            max_util = 0d;

        Double avg_util = Double.parseDouble(cpu_usage_data[3].toString())
                / Double.parseDouble(cpu_requests_data[3].toString());
        if (avg_util == Double.POSITIVE_INFINITY || avg_util == Double.NEGATIVE_INFINITY)
            avg_util = 0d;

        Object[] o = new Object[] {
                "kubernetes.cpu.usage_pct",
                cpu_usage_data[1],
                max_util * 100,
                avg_util * 100
        };
        return o;
    }

    public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent event, final Context context) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Custom-Header", "application/json");

        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent()
                .withHeaders(headers);
        try {
            Gson gson = new Gson();
            System.out.println("Start");
            
            System.out.println("CONTEXT: " + gson.toJson(context));
            // process event
            System.out.println("EVENT: " + gson.toJson(event.getPathParameters().get("id")));
            // e.g. event.getPathParameters() = {"id":"test2"}

            // String pathParamId = gson.toJson(event.pathParameters.id)

            return response
                    .withStatusCode(200)
                    .withBody("success");
        } catch (Exception e) {
            return response
                    .withBody("{}")
                    .withStatusCode(500);
        }
    }

    @Tracing(namespace = "getPageContents")
    private String getPageContents(String address) throws IOException {
        URL url = new URL(address);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()))) {
            return br.lines().collect(Collectors.joining(System.lineSeparator()));
        }
    }
}