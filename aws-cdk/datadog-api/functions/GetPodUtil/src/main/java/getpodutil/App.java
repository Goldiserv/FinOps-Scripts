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

public class App implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    @Tracing(captureMode = DISABLED)
    @Metrics(captureColdStart = true)

    static MetricsQueryResponse queryData(String queryString) {

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
        // e.g. cpu_usage_data = ['kubernetes.cpu.usage.total', 15, 4.771e-06, 3.496e-06]
        // e.g. cpu_requests_data = ['kubernetes.cpu.requests', 15, 0.0010000000474974513, 0.0010000000474974513]
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

            System.out.println("Start");
            Gson gson = new Gson();

            String pod_name = gson.toJson(event.getPathParameters().get("pod_name"));

            String queryString = buildQuery(pod_name);
            // e.g. "ac-exchange-566d997f8-2z7rn"

            MetricsQueryResponse resp = queryData(queryString);
            List<Object[]> response_formatted = formatResponseData(resp);
            // e.g. [["kubernetes.memory.usage_pct",24,65.93246459960938,65.68818092346191],
            // ["kubernetes.memory.usage",24,7.07944448E8,7.05321472E8],
            // ["kubernetes.memory.limits",24,1.073741824E9,1.073741824E9],
            // ["kubernetes.cpu.usage.total",24,0.050148744,0.01724469358333333],
            // ["kubernetes.cpu.requests",24,0.009999999776482582,0.009999999776482582]]

            Object[] cpu_util_row = getCpuUtil(response_formatted.get(3), response_formatted.get(4));
            // System.out.println(gson.toJson(cpu_util_row));
            // e.g. ["kubernetes.cpu.usage_pct",24, 5.1, 4.8]
            response_formatted.add(cpu_util_row);

            String response_formatted_json = gson.toJson(response_formatted);
            
            System.out.println(response_formatted_json);

            System.out.println("Done");

            return response
                    .withStatusCode(200)
                    .withBody(response_formatted_json);
        } catch (Exception e) {
            return response
                    .withBody(e.toString())
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